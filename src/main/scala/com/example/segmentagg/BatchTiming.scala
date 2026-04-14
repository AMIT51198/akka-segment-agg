package com.example.segmentagg

/**
 * Timing instrumentation captured during batch I/O, decoupled from batch data.
 *
 * Immutable — all fields are set at construction time.
 * Derived metrics (decode duration, total read duration, etc.) are computed on access.
 */
final case class BatchTiming(
    fetchStartNanos: Long,
    fetchEndNanos: Long,
    segmentDecodeEndNanos: Long,
    impressionDecodeEndNanos: Long,
    decodeEndNanos: Long
) {
  def fetchNanos: Long            = fetchEndNanos - fetchStartNanos
  def segmentDecodeNanos: Long    = segmentDecodeEndNanos - fetchEndNanos
  def impressionDecodeNanos: Long = impressionDecodeEndNanos - segmentDecodeEndNanos
  def revenueDecodeNanos: Long    = decodeEndNanos - impressionDecodeEndNanos
  def totalDecodeNanos: Long      = decodeEndNanos - fetchEndNanos
  def totalReadNanos: Long        = decodeEndNanos - fetchStartNanos
}

object BatchTiming {

  /** Sentinel value for batches where no timing was recorded. */
  val Zero: BatchTiming = BatchTiming(0L, 0L, 0L, 0L, 0L)
}

