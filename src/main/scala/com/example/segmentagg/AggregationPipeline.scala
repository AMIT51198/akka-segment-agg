package com.example.segmentagg

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardOpenOption}
import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Using

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.Attributes
import akka.stream.scaladsl.Sink

/**
 * Orchestrates: read Parquet → decode → partial aggregate → merge → write CSV.
 * Also owns metrics collection and reporting.
 */
object AggregationPipeline {

  def run(config: PipelineConfig): Unit = {
    val startTime  = Instant.now()
    val startNanos = System.nanoTime()
    val metrics    = new StageMetrics

    log(s"Starting job at $startTime $config")

    implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "segment-aggregation")
    implicit val ec: ExecutionContext          = system.executionContext

    try {
      val result = ParquetBatchSource
        .source(config.input, config.readParallelism)
        .filter(_.size > 0)
        .addAttributes(Attributes.inputBuffer(initial = 1, max = 1))
        .mapAsyncUnordered(config.aggregateParallelism) { batch =>
          Future {
            val aggStart = System.nanoTime()
            val t = batch.timing
            metrics.record("fetch", t.fetchNanos)
            metrics.record("decode", t.totalDecodeNanos)
            metrics.record("read", t.totalReadNanos)
            metrics.record("wait", math.max(0L, aggStart - t.decodeEndNanos))

            val partial = Aggregator.aggregateBatch(batch)
            metrics.record("aggregate", System.nanoTime() - aggStart)
            partial
          }
        }
        .runWith(Sink.fold(Aggregator.emptyAggregate)(_ merge _))

      val finalAgg = Await.result(result, Duration.Inf)
      val rows     = finalAgg.toRows
      val elapsed  = (System.nanoTime() - startNanos) / 1e6

      log(f"Completed: ${rows.size} groups, elapsed=$elapsed%.2f ms")
      metrics.printSummary()
      writeCsv(config.output, rows)
    } finally {
      log("Terminating actor system")
      system.terminate()
      Await.result(system.whenTerminated, Duration.Inf)
    }
  }

  private def writeCsv(path: Path, rows: Vector[AggregateRow]): Unit = {
    Option(path.getParent).foreach(Files.createDirectories(_))
    log(s"Writing ${rows.size} rows to $path")
    Using.resource(
      Files.newBufferedWriter(path, StandardCharsets.UTF_8,
        StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)
    ) { w =>
      w.write("segment_id,total_impressions,total_revenue\n")
      rows.foreach(r => w.write(s"${r.segmentId},${r.totalImpressions},${r.totalRevenue}\n"))
    }
    log(s"Finished writing $path")
  }

  private def log(msg: String): Unit =
    println(s"[${Instant.now()}] [AggregationPipeline] INFO  $msg")
}

// --- Metrics ---

/** Thread-safe per-stage latency collector with summary reporting. */
final class StageMetrics {
  private val stages = Vector("fetch", "decode", "read", "wait", "aggregate")
  private val data   = stages.map(_ -> new ConcurrentLinkedQueue[java.lang.Long]()).toMap

  def record(stage: String, nanos: Long): Unit =
    data.get(stage).foreach(_.add(nanos))

  def printSummary(): Unit = stages.foreach { name =>
    val samples = new scala.collection.mutable.ArrayBuffer[Long]()
    val iter    = data(name).iterator()
    while (iter.hasNext) samples += iter.next().longValue()
    if (samples.isEmpty) {
      println(s"[${Instant.now()}] [AggregationPipeline] INFO  Stage metrics $name=count=0")
    } else {
      val sorted = samples.sorted
      val avg = sorted.sum.toDouble / sorted.size
      val p50 = sorted((sorted.size * 0.50).toInt.min(sorted.size - 1))
      val p95 = sorted((sorted.size * 0.95).toInt.min(sorted.size - 1))
      val max = sorted.last
      println(f"[${Instant.now()}] [AggregationPipeline] INFO  Stage metrics $name=count=${sorted.size} avg=${avg / 1e6}%.2f ms p50=${p50 / 1e6}%.2f ms p95=${p95 / 1e6}%.2f ms max=${max / 1e6}%.2f ms")
    }
  }
}

// --- Output Row ---

final case class AggregateRow(segmentId: Long, totalImpressions: Long, totalRevenue: Double)

