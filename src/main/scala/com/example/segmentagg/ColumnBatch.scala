package com.example.segmentagg

/**
 * An immutable columnar batch of decoded Parquet data.
 *
 * Keeps decoded column arrays separate from instrumentation timing so
 * the aggregation hot-path never needs to know about metrics.
 *
 * Note: arrays are constructed once during decoding and never modified afterwards.
 */
final case class ColumnBatch(
    segmentIds: Array[Long],
    impressions: Array[Long],
    revenues: Array[Double],
    size: Int,
    batchId: Int,
    timing: BatchTiming
)

object ColumnBatch {

  /** Sentinel empty batch with no data and no timing. */
  val Empty: ColumnBatch =
    ColumnBatch(Array.emptyLongArray, Array.emptyLongArray, Array.emptyDoubleArray, 0, -1, BatchTiming.Zero)
}

