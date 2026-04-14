package com.example.segmentagg.metrics

import com.example.segmentagg.logging.PipelineLogger

/**
 * Collects per-stage latency distributions for the aggregation pipeline.
 *
 * Thread-safe — all underlying [[LatencyDistribution]] instances use a lock-free queue.
 */
final class StageMetrics private (
    private val stages: Vector[(String, LatencyDistribution)]
) {

  def apply(name: String): LatencyDistribution =
    stages
      .find(_._1 == name)
      .map(_._2)
      .getOrElse(throw new NoSuchElementException(s"Unknown stage metric: $name"))

  /** Print a formatted summary of every tracked stage. */
  def printSummary(logger: PipelineLogger): Unit =
    stages.foreach { case (name, dist) =>
      logger.info(s"Stage metrics $name=${dist.snapshot().summary}")
    }
}

object StageMetrics {

  /** The fixed set of pipeline stages whose latency we track. */
  def pipelineDefault: StageMetrics = new StageMetrics(
    Vector(
      "fetch"            -> new LatencyDistribution,
      "segmentDecode"    -> new LatencyDistribution,
      "impressionDecode" -> new LatencyDistribution,
      "revenueDecode"    -> new LatencyDistribution,
      "decode"           -> new LatencyDistribution,
      "read"             -> new LatencyDistribution,
      "wait"             -> new LatencyDistribution,
      "aggregate"        -> new LatencyDistribution
    )
  )
}

