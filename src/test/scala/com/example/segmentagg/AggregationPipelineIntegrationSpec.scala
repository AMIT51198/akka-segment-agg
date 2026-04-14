package com.example.segmentagg

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.Attributes
import akka.stream.scaladsl.{Sink, Source}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import com.example.segmentagg.model.AggregateRow

/**
 * Integration tests for the full aggregation pipeline using Akka Streams.
 *
 * These tests exercise the actual stream topology (Source → mapAsyncUnordered → Sink.fold)
 * with real threads, verifying correctness of the concurrent aggregate+merge flow
 * against known expected outputs.
 */
class AggregationPipelineIntegrationSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  implicit var system: ActorSystem[Nothing] = _

  override def beforeAll(): Unit = {
    system = ActorSystem(Behaviors.empty, "test-pipeline")
  }

  override def afterAll(): Unit = {
    system.terminate()
    Await.result(system.whenTerminated, 10.seconds)
  }

  import SegmentRevenueAggregator._
  import TestBatchFactory._

  // -------------------------------------------------------------------------
  // Scenario 1: Simple known input → expected output
  // -------------------------------------------------------------------------

  test("pipeline produces correct aggregation for known input batches") {
    implicit val ec: ExecutionContext = system.executionContext

    // Input: 4 batches with overlapping segment IDs
    //   seg 1: (10+20+30) impressions = 60, (1.0+2.0+3.0) revenue = 6.0
    //   seg 2: (5+25) impressions = 30, (0.5+2.5) revenue = 3.0
    //   seg 3: (15+35) impressions = 50, (1.5+3.5) revenue = 5.0
    val batches = List(
      fromTuples(1, (1L, 10L, 1.0), (2L, 5L, 0.5)),
      fromTuples(2, (1L, 20L, 2.0), (3L, 15L, 1.5)),
      fromTuples(3, (2L, 25L, 2.5), (3L, 35L, 3.5)),
      fromTuples(4, (1L, 30L, 3.0))
    )

    val result = runPipeline(batches, aggregateParallelism = 4)

    result shouldBe Vector(
      AggregateRow(1L, 60L, 6.0),
      AggregateRow(2L, 30L, 3.0),
      AggregateRow(3L, 50L, 5.0)
    )
  }

  // -------------------------------------------------------------------------
  // Scenario 2: Single batch, no merging needed
  // -------------------------------------------------------------------------

  test("pipeline with single batch") {
    implicit val ec: ExecutionContext = system.executionContext

    val batches = List(
      fromTuples(1,
        (10L, 100L, 10.0),
        (20L, 200L, 20.0),
        (10L, 50L,  5.0)
      )
    )

    val result = runPipeline(batches, aggregateParallelism = 2)

    result shouldBe Vector(
      AggregateRow(10L, 150L, 15.0),
      AggregateRow(20L, 200L, 20.0)
    )
  }

  // -------------------------------------------------------------------------
  // Scenario 3: Empty input
  // -------------------------------------------------------------------------

  test("pipeline with no batches produces empty result") {
    implicit val ec: ExecutionContext = system.executionContext

    val result = runPipeline(List.empty, aggregateParallelism = 4)
    result shouldBe empty
  }

  // -------------------------------------------------------------------------
  // Scenario 4: All batches have same segment → single output row
  // -------------------------------------------------------------------------

  test("pipeline with all-same-segment batches") {
    implicit val ec: ExecutionContext = system.executionContext

    val batches = (1 to 100).map { i =>
      fromTuples(i, (42L, 1L, 0.01))
    }.toList

    val result = runPipeline(batches, aggregateParallelism = 8)

    result should have size 1
    result.head.segmentId shouldBe 42L
    result.head.totalImpressions shouldBe 100L
    result.head.totalRevenue shouldBe (1.0 +- 0.01)
  }

  // -------------------------------------------------------------------------
  // Scenario 5: High parallelism stress test
  // -------------------------------------------------------------------------

  test("pipeline with high parallelism produces correct results") {
    implicit val ec: ExecutionContext = system.executionContext

    // 200 batches, each with 10 rows across 5 segments
    val batches = (0 until 200).map { i =>
      fromTuples(i,
        (1L, 1L, 0.1),
        (2L, 2L, 0.2),
        (3L, 3L, 0.3),
        (4L, 4L, 0.4),
        (5L, 5L, 0.5),
        (1L, 1L, 0.1),
        (2L, 2L, 0.2),
        (3L, 3L, 0.3),
        (4L, 4L, 0.4),
        (5L, 5L, 0.5)
      )
    }.toList

    val result = runPipeline(batches, aggregateParallelism = 12)

    result should have size 5

    // Each segment appears 2× per batch, 200 batches = 400 occurrences
    result.foreach { row =>
      row.totalImpressions shouldBe row.segmentId * 400
      row.totalRevenue shouldBe (row.segmentId * 0.1 * 400 +- 0.1)
    }
  }

  // -------------------------------------------------------------------------
  // Scenario 6: Mixed empty + non-empty batches
  // -------------------------------------------------------------------------

  test("pipeline filters empty batches and still produces correct results") {
    implicit val ec: ExecutionContext = system.executionContext

    val batches = List(
      ColumnBatch.Empty,
      fromTuples(2, (1L, 10L, 1.0)),
      ColumnBatch.Empty,
      ColumnBatch.Empty,
      fromTuples(5, (1L, 20L, 2.0), (2L, 30L, 3.0)),
      ColumnBatch.Empty
    )

    val result = runPipeline(batches, aggregateParallelism = 4)

    result shouldBe Vector(
      AggregateRow(1L, 30L, 3.0),
      AggregateRow(2L, 30L, 3.0)
    )
  }

  // -------------------------------------------------------------------------
  // Scenario 7: Determinism check — run same input multiple times
  // -------------------------------------------------------------------------

  test("pipeline produces same result across repeated runs despite thread scheduling") {
    implicit val ec: ExecutionContext = system.executionContext

    val batches = (0 until 50).map { i =>
      fromTuples(i,
        ((i % 10).toLong, 1L, 0.1),
        ((i % 10 + 10).toLong, 2L, 0.2)
      )
    }.toList

    // Run 5 times and collect results
    val runs = (1 to 5).map(_ => runPipeline(batches, aggregateParallelism = 8))

    // All runs should produce identical results
    runs.foreach { result =>
      result shouldBe runs.head
    }
  }

  // -------------------------------------------------------------------------
  // Helper: runs the actual Akka Streams pipeline topology
  // -------------------------------------------------------------------------

  private def runPipeline(
      batches: List[ColumnBatch],
      aggregateParallelism: Int
  )(implicit ec: ExecutionContext): Vector[AggregateRow] = {
    val future = Source(batches)
      .filter(_.size > 0)
      .addAttributes(Attributes.inputBuffer(initial = 1, max = 1))
      .mapAsyncUnordered(aggregateParallelism) { batch =>
        Future(aggregateBatch(batch))
      }
      .runWith(Sink.fold(PartialAggregate.empty)(_ merge _))
      .map(_.rows)

    Await.result(future, 30.seconds)
  }
}

