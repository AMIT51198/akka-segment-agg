package com.example.segmentagg

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for [[ColumnBatch]] and [[BatchTiming]] data classes.
 */
class ColumnBatchSpec extends AnyFunSuite with Matchers {

  test("Empty batch has zero size and sentinel batchId") {
    val e = ColumnBatch.Empty
    e.size shouldBe 0
    e.batchId shouldBe -1
    e.segmentIds shouldBe Array.emptyLongArray
    e.impressions shouldBe Array.emptyLongArray
    e.revenues shouldBe Array.emptyDoubleArray
    e.timing shouldBe BatchTiming.Zero
  }

  test("BatchTiming.Zero has all zero derived metrics") {
    val z = BatchTiming.Zero
    z.fetchNanos shouldBe 0L
    z.segmentDecodeNanos shouldBe 0L
    z.impressionDecodeNanos shouldBe 0L
    z.revenueDecodeNanos shouldBe 0L
    z.totalDecodeNanos shouldBe 0L
    z.totalReadNanos shouldBe 0L
  }

  test("BatchTiming computes correct derived durations") {
    val t = BatchTiming(
      fetchStartNanos          = 100L,
      fetchEndNanos            = 200L,
      segmentDecodeEndNanos    = 350L,
      impressionDecodeEndNanos = 400L,
      decodeEndNanos           = 600L
    )

    t.fetchNanos shouldBe 100L          // 200 - 100
    t.segmentDecodeNanos shouldBe 150L  // 350 - 200
    t.impressionDecodeNanos shouldBe 50L // 400 - 350
    t.revenueDecodeNanos shouldBe 200L  // 600 - 400
    t.totalDecodeNanos shouldBe 400L    // 600 - 200
    t.totalReadNanos shouldBe 500L      // 600 - 100
  }

  test("ColumnBatch constructed from arrays preserves data") {
    val batch = TestBatchFactory.fromTuples(42,
      (1L, 10L, 1.5),
      (2L, 20L, 2.5),
      (3L, 30L, 3.5)
    )

    batch.size shouldBe 3
    batch.batchId shouldBe 42
    batch.segmentIds shouldBe Array(1L, 2L, 3L)
    batch.impressions shouldBe Array(10L, 20L, 30L)
    batch.revenues shouldBe Array(1.5, 2.5, 3.5)
  }
}

