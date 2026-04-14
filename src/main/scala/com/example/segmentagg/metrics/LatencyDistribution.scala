package com.example.segmentagg.metrics

import java.util.concurrent.ConcurrentLinkedQueue

/**
 * Nanosecond-level latency distribution collector.
 *
 * Thread-safe via a lock-free [[ConcurrentLinkedQueue]].
 */
final class LatencyDistribution {
  private val samples = new ConcurrentLinkedQueue[java.lang.Long]()

  def record(nanos: Long): Unit = samples.add(nanos)

  /** Take a consistent snapshot of all samples collected so far. */
  def snapshot(): LatencyDistribution.Snapshot = {
    val buffer = scala.collection.mutable.ArrayBuffer.empty[Long]
    val iter   = samples.iterator()
    while (iter.hasNext) {
      buffer += iter.next().longValue()
    }
    LatencyDistribution.Snapshot(buffer.sorted.toIndexedSeq)
  }
}

object LatencyDistribution {

  /**
   * Immutable snapshot of a latency distribution at a point in time.
   *
   * Provides standard percentile-based summary statistics.
   */
  final case class Snapshot(sorted: IndexedSeq[Long]) {

    def count: Int       = sorted.size
    def isEmpty: Boolean = sorted.isEmpty

    def summary: String =
      if (sorted.isEmpty) "count=0"
      else {
        val avg = sorted.sum.toDouble / sorted.size
        val p50 = percentile(0.50)
        val p95 = percentile(0.95)
        val max = sorted.last
        f"count=${sorted.size} avg=${avg / 1e6}%.2f ms p50=${p50 / 1e6}%.2f ms p95=${p95 / 1e6}%.2f ms max=${max / 1e6}%.2f ms"
      }

    private def percentile(p: Double): Long = {
      val index = math.min(sorted.size - 1, math.max(0, math.ceil(sorted.size * p).toInt - 1))
      sorted(index)
    }
  }

  /** Format nanoseconds as a human-readable millisecond string. */
  def formatMillis(nanos: Long): String = f"${nanos / 1e6}%.2f ms"
}

