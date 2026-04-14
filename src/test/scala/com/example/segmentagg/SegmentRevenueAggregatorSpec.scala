package com.example.segmentagg

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.example.segmentagg.model.AggregateRow

/**
 * Tests for [[SegmentRevenueAggregator]].
 *
 * Covers: single batch, multiple segments, merging partial aggregates,
 * empty batches, duplicate segment IDs, floating-point accumulation,
 * and concurrent multi-threaded aggregation + merge.
 */
class SegmentRevenueAggregatorSpec extends AnyFunSuite with Matchers {

  import SegmentRevenueAggregator._
  import TestBatchFactory._

  // -------------------------------------------------------------------------
  // Single batch aggregation
  // -------------------------------------------------------------------------

  test("single row batch produces one aggregate row") {
    val batch = fromTuples(1, (100L, 5L, 1.50))
    val result = aggregateBatch(batch).rows

    result shouldBe Vector(AggregateRow(100L, 5L, 1.50))
  }

  test("multiple rows with distinct segment IDs") {
    val batch = fromTuples(1,
      (1L, 10L, 1.0),
      (2L, 20L, 2.0),
      (3L, 30L, 3.0)
    )
    val result = aggregateBatch(batch).rows

    result shouldBe Vector(
      AggregateRow(1L, 10L, 1.0),
      AggregateRow(2L, 20L, 2.0),
      AggregateRow(3L, 30L, 3.0)
    )
  }

  test("duplicate segment IDs within a single batch are summed") {
    val batch = fromTuples(1,
      (1L, 10L, 1.0),
      (1L, 20L, 2.0),
      (1L, 30L, 3.0),
      (2L, 5L,  0.5)
    )
    val result = aggregateBatch(batch).rows

    result shouldBe Vector(
      AggregateRow(1L, 60L, 6.0),
      AggregateRow(2L, 5L,  0.5)
    )
  }

  test("empty batch produces empty result") {
    val batch = ColumnBatch.Empty
    val result = aggregateBatch(batch).rows

    result shouldBe empty
  }

  test("batch with zero impressions and zero revenue") {
    val batch = fromTuples(1,
      (1L, 0L, 0.0),
      (2L, 0L, 0.0)
    )
    val result = aggregateBatch(batch).rows

    result shouldBe Vector(
      AggregateRow(1L, 0L, 0.0),
      AggregateRow(2L, 0L, 0.0)
    )
  }

  test("large impressions and revenue values do not overflow") {
    val batch = fromTuples(1,
      (1L, Long.MaxValue / 2, 1e15),
      (1L, Long.MaxValue / 2, 1e15)
    )
    val result = aggregateBatch(batch).rows

    result should have size 1
    result.head.segmentId shouldBe 1L
    result.head.totalImpressions shouldBe (Long.MaxValue / 2) * 2
    result.head.totalRevenue shouldBe 2e15
  }

  // -------------------------------------------------------------------------
  // Merging partial aggregates
  // -------------------------------------------------------------------------

  test("merging two disjoint partial aggregates") {
    val batch1 = fromTuples(1, (1L, 10L, 1.0))
    val batch2 = fromTuples(2, (2L, 20L, 2.0))

    val merged = aggregateBatch(batch1).merge(aggregateBatch(batch2)).rows

    merged shouldBe Vector(
      AggregateRow(1L, 10L, 1.0),
      AggregateRow(2L, 20L, 2.0)
    )
  }

  test("merging two overlapping partial aggregates sums values") {
    val batch1 = fromTuples(1,
      (1L, 10L, 1.0),
      (2L, 20L, 2.0)
    )
    val batch2 = fromTuples(2,
      (1L, 30L, 3.0),
      (3L, 40L, 4.0)
    )

    val merged = aggregateBatch(batch1).merge(aggregateBatch(batch2)).rows

    merged shouldBe Vector(
      AggregateRow(1L, 40L, 4.0),   // 10+30, 1.0+3.0
      AggregateRow(2L, 20L, 2.0),
      AggregateRow(3L, 40L, 4.0)
    )
  }

  test("merging with empty partial aggregate is identity") {
    val batch = fromTuples(1, (1L, 10L, 1.0))
    val partial = aggregateBatch(batch)

    val merged = partial.merge(PartialAggregate.empty).rows

    merged shouldBe Vector(AggregateRow(1L, 10L, 1.0))
  }

  test("merging empty into empty is empty") {
    val merged = PartialAggregate.empty.merge(PartialAggregate.empty).rows
    merged shouldBe empty
  }

  test("fold-style merge across multiple batches (simulates pipeline Sink.fold)") {
    val batches = List(
      fromTuples(1, (1L, 10L, 1.0), (2L, 5L, 0.5)),
      fromTuples(2, (1L, 20L, 2.0), (3L, 15L, 1.5)),
      fromTuples(3, (2L, 25L, 2.5), (3L, 35L, 3.5)),
      fromTuples(4, (1L, 30L, 3.0))
    )

    val result = batches
      .map(aggregateBatch)
      .foldLeft(PartialAggregate.empty)(_ merge _)
      .rows

    result shouldBe Vector(
      AggregateRow(1L, 60L, 6.0),    // 10+20+30, 1+2+3
      AggregateRow(2L, 30L, 3.0),    // 5+25, 0.5+2.5
      AggregateRow(3L, 50L, 5.0)     // 15+35, 1.5+3.5
    )
  }

  // -------------------------------------------------------------------------
  // Floating-point precision
  // -------------------------------------------------------------------------

  test("floating-point revenue accumulation is reasonably precise") {
    // 1000 rows all adding 0.1 — classic floating-point trap
    val data = (1 to 1000).map(i => (1L, 1L, 0.1))
    val batch = fromTuples(1, data: _*)
    val result = aggregateBatch(batch).rows

    result should have size 1
    result.head.totalImpressions shouldBe 1000L
    // Allow small floating-point drift
    result.head.totalRevenue shouldBe (100.0 +- 0.01)
  }

  // -------------------------------------------------------------------------
  // Results are sorted by segmentId
  // -------------------------------------------------------------------------

  test("output rows are sorted by segmentId regardless of input order") {
    val batch = fromTuples(1,
      (999L, 1L, 0.1),
      (1L,   2L, 0.2),
      (500L, 3L, 0.3),
      (42L,  4L, 0.4)
    )
    val result = aggregateBatch(batch).rows

    result.map(_.segmentId) shouldBe Vector(1L, 42L, 500L, 999L)
  }

  // -------------------------------------------------------------------------
  // Multi-threaded concurrent aggregation + merge
  // -------------------------------------------------------------------------

  test("concurrent aggregation across multiple threads produces correct results") {
    // Simulate the pipeline: N threads each aggregate a batch, then merge
    val batchCount = 100
    val batches = (0 until batchCount).map { i =>
      fromTuples(i,
        (1L, 1L, 0.01),
        (2L, 2L, 0.02),
        (3L, 3L, 0.03)
      )
    }

    import scala.concurrent.{Await, Future}
    import scala.concurrent.duration._
    import scala.concurrent.ExecutionContext.Implicits.global

    // Aggregate each batch on a different thread
    val futurePartials: Seq[Future[PartialAggregate]] =
      batches.map(batch => Future(aggregateBatch(batch)))

    val partials = Await.result(Future.sequence(futurePartials), 30.seconds)

    // Sequential merge (same as Sink.fold in the pipeline)
    val result = partials.foldLeft(PartialAggregate.empty)(_ merge _).rows

    result should have size 3
    result.map(_.segmentId) shouldBe Vector(1L, 2L, 3L)
    result(0).totalImpressions shouldBe 1L * batchCount
    result(1).totalImpressions shouldBe 2L * batchCount
    result(2).totalImpressions shouldBe 3L * batchCount
    result(0).totalRevenue shouldBe (0.01 * batchCount +- 0.01)
    result(1).totalRevenue shouldBe (0.02 * batchCount +- 0.01)
    result(2).totalRevenue shouldBe (0.03 * batchCount +- 0.01)
  }

  test("concurrent aggregation with high segment cardinality") {
    // 10 threads, each batch has 1000 unique segments
    val threadCount = 10

    import scala.concurrent.{Await, Future}
    import scala.concurrent.duration._
    import scala.concurrent.ExecutionContext.Implicits.global

    val futures = (0 until threadCount).map { t =>
      Future {
        val data = (0 until 1000).map { s =>
          (s.toLong, (t + 1).toLong, (t + 1).toDouble * 0.01)
        }
        aggregateBatch(fromTuples(t, data: _*))
      }
    }

    val partials = Await.result(Future.sequence(futures), 30.seconds)
    val result = partials.foldLeft(PartialAggregate.empty)(_ merge _).rows

    result should have size 1000

    // Each segment should have impressions = sum(1..10) = 55
    // Each segment should have revenue = sum(1..10) * 0.01 = 0.55
    result.foreach { row =>
      row.totalImpressions shouldBe 55L
      row.totalRevenue shouldBe (0.55 +- 0.001)
    }
  }

  test("parallel aggregate + merge race condition stress test") {
    // Many threads doing aggregation and we merge results
    // This tests that PartialAggregate.merge is safe when called sequentially
    // after concurrent construction
    val threadCount = 50
    val rowsPerBatch = 500

    import scala.concurrent.{Await, Future}
    import scala.concurrent.duration._
    import java.util.concurrent.Executors
    implicit val ec: scala.concurrent.ExecutionContext =
      scala.concurrent.ExecutionContext.fromExecutor(Executors.newFixedThreadPool(threadCount))

    val futures = (0 until threadCount).map { t =>
      Future {
        val data = (0 until rowsPerBatch).map { r =>
          val segId = (r % 50).toLong  // 50 unique segments
          (segId, 1L, 0.01)
        }
        aggregateBatch(fromTuples(t, data: _*))
      }
    }

    val partials = Await.result(Future.sequence(futures), 60.seconds)
    val result = partials.foldLeft(PartialAggregate.empty)(_ merge _).rows

    result should have size 50

    // Each segment appears rowsPerBatch/50 = 10 times per batch, across 50 batches
    val expectedImpressions = (rowsPerBatch / 50) * threadCount  // 10 * 50 = 500
    val expectedRevenue = (rowsPerBatch / 50) * threadCount * 0.01  // 5.0

    result.foreach { row =>
      row.totalImpressions shouldBe expectedImpressions
      row.totalRevenue shouldBe (expectedRevenue +- 0.01)
    }
  }
}

