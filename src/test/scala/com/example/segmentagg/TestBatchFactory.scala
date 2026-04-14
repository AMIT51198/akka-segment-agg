package com.example.segmentagg

/**
 * Test helpers for constructing [[ColumnBatch]] instances from concise input data.
 */
object TestBatchFactory {

  /** Build a batch from tuples of (segmentId, impressions, revenue). */
  def fromTuples(batchId: Int, data: (Long, Long, Double)*): ColumnBatch = {
    val segmentIds  = data.map(_._1).toArray
    val impressions = data.map(_._2).toArray
    val revenues    = data.map(_._3).toArray
    ColumnBatch(segmentIds, impressions, revenues, data.size, batchId, BatchTiming.Zero)
  }

  /** Build a batch with realistic timing attached. */
  def withTiming(batchId: Int, timing: BatchTiming, data: (Long, Long, Double)*): ColumnBatch = {
    val segmentIds  = data.map(_._1).toArray
    val impressions = data.map(_._2).toArray
    val revenues    = data.map(_._3).toArray
    ColumnBatch(segmentIds, impressions, revenues, data.size, batchId, timing)
  }
}

