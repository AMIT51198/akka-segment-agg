package com.example.segmentagg

import java.time.Instant

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.Attributes
import akka.stream.scaladsl.Sink

import com.example.segmentagg.io.{CsvResultWriter, ResultWriter}
import com.example.segmentagg.logging.PipelineLogger
import com.example.segmentagg.metrics.{LatencyDistribution, StageMetrics}
import com.example.segmentagg.model.PipelineConfig
import com.example.segmentagg.parquet.DecoderPageMetrics

/**
 * Orchestrates the segment-revenue aggregation pipeline.
 *
 * Wires the Akka Streams stages together and delegates metrics collection,
 * aggregation, and result writing to injected collaborators.
 * New output formats can be added by providing a different [[ResultWriter]];
 * new decode strategies by extending [[com.example.segmentagg.parquet.DecodeMode]].
 *
 * @param config       pipeline configuration (input path, parallelism, decode mode)
 * @param logger       structured logger
 * @param stageMetrics per-stage latency collector
 * @param pageMetrics  per-column page-level decode latency collector
 * @param resultWriter where the final aggregated rows are written
 */
final class AggregationPipeline(
    config: PipelineConfig,
    logger: PipelineLogger,
    stageMetrics: StageMetrics,
    pageMetrics: DecoderPageMetrics,
    resultWriter: ResultWriter
) {

  /**
   * Execute the pipeline: read → decode → aggregate → materialise → write.
   *
   * Blocks until the pipeline completes; manages the [[ActorSystem]] lifecycle.
   */
  def run(): Unit = {
    val pipelineStart      = Instant.now()
    val pipelineStartNanos = System.nanoTime()

    logger.info(s"Starting job at $pipelineStart $config")

    implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "segment-aggregation")
    implicit val ec: ExecutionContext          = system.executionContext

    try {
      val aggregation = buildStream()
      val completed   = aggregation.map(finalAggregate => reportResults(finalAggregate, pipelineStartNanos))
      Await.result(completed, Duration.Inf)
    } finally {
      shutdownActorSystem(system)
    }
  }

  // -- private helpers (each with a single, clear responsibility) --

  private def buildStream()(implicit system: ActorSystem[Nothing], ec: ExecutionContext) =
    ParquetBatchSource
      .source(config.input, config.readParallelism, config.decodeMode)
      .filter(_.size > 0)
      .addAttributes(Attributes.inputBuffer(initial = 1, max = 1))
      .mapAsyncUnordered(config.aggregateParallelism) { batch =>
        Future(recordAndAggregate(batch))
      }
      .runWith(Sink.fold(SegmentRevenueAggregator.PartialAggregate.empty)(_ merge _))

  private def recordAndAggregate(batch: ColumnBatch): SegmentRevenueAggregator.PartialAggregate = {
    val aggregateStartNanos = System.nanoTime()
    val t = batch.timing

    stageMetrics("fetch").record(t.fetchNanos)
    stageMetrics("segmentDecode").record(t.segmentDecodeNanos)
    stageMetrics("impressionDecode").record(t.impressionDecodeNanos)
    stageMetrics("revenueDecode").record(t.revenueDecodeNanos)
    stageMetrics("decode").record(t.totalDecodeNanos)
    stageMetrics("read").record(t.totalReadNanos)
    stageMetrics("wait").record(math.max(0L, aggregateStartNanos - t.decodeEndNanos))

    val partial = SegmentRevenueAggregator.aggregateBatch(batch)
    stageMetrics("aggregate").record(System.nanoTime() - aggregateStartNanos)
    partial
  }

  private def reportResults(
      finalAggregate: SegmentRevenueAggregator.PartialAggregate,
      pipelineStartNanos: Long
  ): Unit = {
    val materializeStart   = System.nanoTime()
    val rows               = finalAggregate.rows
    val materializeElapsed = System.nanoTime() - materializeStart
    val pipelineElapsed    = System.nanoTime() - pipelineStartNanos

    logger.info(
      s"Final aggregate materialised at ${Instant.now()} " +
        s"with ${finalAggregate.size} groups and ${rows.size} rows " +
        s"(final materialise=${LatencyDistribution.formatMillis(materializeElapsed)}, " +
        s"total elapsed=${LatencyDistribution.formatMillis(pipelineElapsed)})"
    )

    stageMetrics.printSummary(logger)
    pageMetrics.printSummary(logger)

    resultWriter.write(rows)
  }

  private def shutdownActorSystem(system: ActorSystem[Nothing]): Unit = {
    logger.info("Terminating actor system")
    system.terminate()
    Await.result(system.whenTerminated, Duration.Inf)
    logger.info("Actor system terminated")
  }
}

/**
 * Companion providing a one-shot entry point that constructs the pipeline
 * with production collaborators and runs it.
 */
object AggregationPipeline {

  /** Backward-compatible entry point. */
  def run(config: PipelineConfig): Unit = {
    val logger       = PipelineLogger.console("AggregationPipeline")
    val stageMetrics = StageMetrics.pipelineDefault
    val pageMetrics  = DecoderPageMetrics.reset()
    val writer       = new CsvResultWriter(config.output, logger)

    new AggregationPipeline(config, logger, stageMetrics, pageMetrics, writer).run()
  }
}

