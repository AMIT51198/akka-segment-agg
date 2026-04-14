package com.example.segmentagg.parquet

import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._

import com.example.segmentagg.logging.PipelineLogger
import com.example.segmentagg.metrics.LatencyDistribution

/**
 * Per-column page-level decode latency tracker (thread-safe).
 *
 * Each pipeline run gets its own isolated instance via [[DecoderPageMetrics.reset]],
 * avoiding any shared state between runs.
 */
final class DecoderPageMetrics {
  private val distributions = new ConcurrentHashMap[String, LatencyDistribution]()

  def recordPage(columnName: String, nanos: Long): Unit =
    distributions
      .computeIfAbsent(columnName, _ => new LatencyDistribution)
      .record(nanos)

  def printSummary(logger: PipelineLogger, prefix: String = "Page decode metrics"): Unit =
    distributions.asScala.toSeq.sortBy(_._1).foreach { case (col, dist) =>
      logger.info(s"$prefix $col=${dist.snapshot().summary}")
    }
}

/**
 * Global convenience accessor for the current pipeline run's metrics instance.
 */
object DecoderPageMetrics {
  @volatile private var _instance: DecoderPageMetrics = new DecoderPageMetrics

  def reset(): DecoderPageMetrics = {
    val fresh = new DecoderPageMetrics
    _instance = fresh
    fresh
  }

  def global: DecoderPageMetrics = _instance

  def recordPage(columnName: String, nanos: Long): Unit = _instance.recordPage(columnName, nanos)

  def printSummary(prefix: String = "[AggregationPipeline] Page decode metrics"): Unit = {
    val logger = com.example.segmentagg.logging.PipelineLogger.console("PageMetrics")
    _instance.printSummary(logger, prefix)
  }
}

