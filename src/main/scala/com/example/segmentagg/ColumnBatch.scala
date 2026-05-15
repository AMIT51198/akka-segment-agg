package com.example.segmentagg

/** Decoded columnar batch from one Parquet row group. */
final case class ColumnBatch(
    segmentIds: Array[Long],
    impressions: Array[Long],
    revenues: Array[Double],
    size: Int,
    timing: BatchTiming
)

/** Timing instrumentation captured during batch read + decode. */
final case class BatchTiming(
    fetchStartNanos: Long,
    fetchEndNanos: Long,
    decodeEndNanos: Long
) {
  def fetchNanos: Long       = fetchEndNanos - fetchStartNanos
  def totalDecodeNanos: Long = decodeEndNanos - fetchEndNanos
  def totalReadNanos: Long   = decodeEndNanos - fetchStartNanos
}

object BatchTiming {
  val Zero: BatchTiming = BatchTiming(0L, 0L, 0L)
}

