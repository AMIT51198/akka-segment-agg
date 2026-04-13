package com.example.segmentagg

import java.nio.file.Path
import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.Attributes
import akka.stream.scaladsl.Sink

object AggregationPipeline {
  final case class PipelineConfig(
      input: Path,
      output: Path,
      readParallelism: Int,
      aggregateParallelism: Int
  )

  def run(config: PipelineConfig): Unit = {
    val pipelineStart = Instant.now()
    val pipelineStartNanos = System.nanoTime()
    val metrics = new StageMetrics
    println(
      s"[AggregationPipeline] Starting job at $pipelineStart input=${config.input} output=${config.output} " +
        s"readParallelism=${config.readParallelism} aggregateParallelism=${config.aggregateParallelism}"
    )
    implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "segment-aggregation")
    implicit val ec: ExecutionContext = system.executionContext

    try {
      val aggregation =
        ParquetBatchSource
          .source(config.input, config.readParallelism)
          .filter(_.size > 0)
          .addAttributes(Attributes.inputBuffer(initial = 1, max = 1))
          .mapAsyncUnordered(config.aggregateParallelism) { batch =>
            Future {
              val aggregateStartNanos = System.nanoTime()
              metrics.recordRead(batch.readEndNanos - batch.readStartNanos)
              metrics.recordWait(aggregateStartNanos - batch.readEndNanos)
              val partial = SegmentRevenueAggregator.aggregateBatch(batch)
              val aggregateEndNanos = System.nanoTime()
              metrics.recordAggregate(aggregateEndNanos - aggregateStartNanos)
              partial
            }
          }
          .runWith(Sink.fold(SegmentRevenueAggregator.PartialAggregate.empty)(_ merge _))

      val completed = aggregation.map { finalAggregate =>
        val materializeStart = System.nanoTime()
        val rows = finalAggregate.rows
        val materializeElapsed = System.nanoTime() - materializeStart
        val pipelineElapsed = System.nanoTime() - pipelineStartNanos
        val completedAt = Instant.now()

        println(
          s"[AggregationPipeline] Final aggregate materialized at $completedAt with ${finalAggregate.size} groups and ${rows.size} rows " +
            s"(final materialize=${formatMillis(materializeElapsed)}, total elapsed=${formatMillis(pipelineElapsed)})"
        )
        metrics.printSummary()
        println(s"[AggregationPipeline] CSV write skipped; result kept materialized in memory only")
      }

      Await.result(completed, Duration.Inf)
    } finally {
      println("[AggregationPipeline] Terminating actor system")
      system.terminate()
      Await.result(system.whenTerminated, Duration.Inf)
      println("[AggregationPipeline] Actor system terminated")
    }
  }

  private def formatMillis(nanos: Long): String = f"${nanos / 1000000.0}%.2f ms"

  private final class StageMetrics {
    private val readNanos = new ConcurrentLinkedQueue[java.lang.Long]()
    private val waitNanos = new ConcurrentLinkedQueue[java.lang.Long]()
    private val aggregateNanos = new ConcurrentLinkedQueue[java.lang.Long]()

    def recordRead(nanos: Long): Unit = readNanos.add(nanos)
    def recordWait(nanos: Long): Unit = waitNanos.add(math.max(0L, nanos))
    def recordAggregate(nanos: Long): Unit = aggregateNanos.add(nanos)

    def printSummary(): Unit = {
      println(s"[AggregationPipeline] Stage metrics read=${summary(readNanos)}")
      println(s"[AggregationPipeline] Stage metrics wait=${summary(waitNanos)}")
      println(s"[AggregationPipeline] Stage metrics aggregate=${summary(aggregateNanos)}")
    }

    private def summary(values: ConcurrentLinkedQueue[java.lang.Long]): String = {
      val data = values.iterator()
      val buffer = scala.collection.mutable.ArrayBuffer.empty[Long]
      while (data.hasNext) {
        buffer += data.next().longValue()
      }

      if (buffer.isEmpty) {
        "count=0"
      } else {
        val sorted = buffer.sorted
        val count = sorted.size
        val avg = sorted.sum.toDouble / count
        val p50 = percentile(sorted, 0.50)
        val p95 = percentile(sorted, 0.95)
        val max = sorted.last
        f"count=$count avg=${avg / 1000000.0}%.2f ms p50=${p50 / 1000000.0}%.2f ms p95=${p95 / 1000000.0}%.2f ms max=${max / 1000000.0}%.2f ms"
      }
    }

    private def percentile(sorted: scala.collection.IndexedSeq[Long], p: Double): Long = {
      val index = math.min(sorted.size - 1, math.ceil(sorted.size * p).toInt - 1)
      sorted(index)
    }
  }
}
