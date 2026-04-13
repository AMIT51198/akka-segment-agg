package com.example.segmentagg

final case class ColumnBatch(
    segmentIds: Array[Long],
    impressions: Array[Long],
    revenues: Array[Double],
    size: Int,
    batchId: Int,
    readStartNanos: Long,
    readEndNanos: Long
)

object ColumnBatch {
  def empty: ColumnBatch =
    ColumnBatch(Array.emptyLongArray, Array.emptyLongArray, Array.emptyDoubleArray, 0, -1, 0L, 0L)
}
