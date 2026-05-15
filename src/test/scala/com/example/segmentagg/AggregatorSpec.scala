package com.example.segmentagg

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AggregatorSpec extends AnyFlatSpec with Matchers {

  private def batch(segments: Array[Long], imps: Array[Long], revs: Array[Double]): ColumnBatch =
    ColumnBatch(segments, imps, revs, segments.length, BatchTiming.Zero)

  "Aggregator" should "aggregate a single batch correctly" in {
    val b = batch(
      Array(1L, 2L, 1L, 3L, 2L),
      Array(10L, 20L, 30L, 40L, 50L),
      Array(1.0, 2.0, 3.0, 4.0, 5.0)
    )
    val rows = Aggregator.aggregateBatch(b).toRows

    rows.size shouldBe 3
    rows.find(_.segmentId == 1L).get.totalImpressions shouldBe 40L
    rows.find(_.segmentId == 1L).get.totalRevenue shouldBe 4.0
    rows.find(_.segmentId == 2L).get.totalImpressions shouldBe 70L
    rows.find(_.segmentId == 3L).get.totalImpressions shouldBe 40L
  }

  it should "merge two partial aggregates" in {
    val b1 = batch(Array(1L, 2L), Array(10L, 20L), Array(1.0, 2.0))
    val b2 = batch(Array(2L, 3L), Array(30L, 40L), Array(3.0, 4.0))

    val merged = Aggregator.aggregateBatch(b1).merge(Aggregator.aggregateBatch(b2))
    val rows   = merged.toRows

    rows.size shouldBe 3
    rows.find(_.segmentId == 2L).get.totalImpressions shouldBe 50L
    rows.find(_.segmentId == 2L).get.totalRevenue shouldBe 5.0
  }

  it should "handle empty batch" in {
    val b = batch(Array.empty, Array.empty, Array.empty)
    Aggregator.aggregateBatch(b).toRows shouldBe empty
  }

  it should "merge with empty aggregate" in {
    val b = batch(Array(1L), Array(10L), Array(1.0))
    val partial = Aggregator.aggregateBatch(b)
    val merged  = Aggregator.emptyAggregate.merge(partial)
    merged.toRows.size shouldBe 1
  }

  it should "produce sorted output by segment_id" in {
    val b = batch(Array(5L, 1L, 3L), Array(1L, 1L, 1L), Array(1.0, 1.0, 1.0))
    val rows = Aggregator.aggregateBatch(b).toRows
    rows.map(_.segmentId) shouldBe Vector(1L, 3L, 5L)
  }
}

