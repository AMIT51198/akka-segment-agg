package com.example.segmentagg.metrics

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Tests for [[LatencyDistribution]] — including thread-safety.
 */
class LatencyDistributionSpec extends AnyFunSuite with Matchers {

  test("empty distribution has count=0") {
    val dist = new LatencyDistribution
    val snap = dist.snapshot()

    snap.count shouldBe 0
    snap.isEmpty shouldBe true
    snap.summary shouldBe "count=0"
  }

  test("single sample produces correct snapshot") {
    val dist = new LatencyDistribution
    dist.record(1_000_000L)  // 1 ms

    val snap = dist.snapshot()
    snap.count shouldBe 1
    snap.isEmpty shouldBe false
    snap.sorted shouldBe IndexedSeq(1_000_000L)
  }

  test("multiple samples are sorted in snapshot") {
    val dist = new LatencyDistribution
    dist.record(300L)
    dist.record(100L)
    dist.record(200L)

    val snap = dist.snapshot()
    snap.count shouldBe 3
    snap.sorted shouldBe IndexedSeq(100L, 200L, 300L)
  }

  test("summary format includes count, avg, p50, p95, max") {
    val dist = new LatencyDistribution
    (1 to 100).foreach(i => dist.record(i * 1_000_000L))

    val summary = dist.snapshot().summary
    summary should include("count=100")
    summary should include("avg=")
    summary should include("p50=")
    summary should include("p95=")
    summary should include("max=")
    summary should include("ms")
  }

  test("formatMillis formats nanoseconds to ms with 2 decimal places") {
    LatencyDistribution.formatMillis(1_500_000L) shouldBe "1.50 ms"
    LatencyDistribution.formatMillis(0L) shouldBe "0.00 ms"
    LatencyDistribution.formatMillis(123_456_789L) shouldBe "123.46 ms"
  }

  // -------------------------------------------------------------------------
  // Thread-safety
  // -------------------------------------------------------------------------

  test("concurrent writes from multiple threads all recorded") {
    val dist = new LatencyDistribution
    val threadCount = 20
    val samplesPerThread = 1000

    val futures = (0 until threadCount).map { t =>
      Future {
        (0 until samplesPerThread).foreach { i =>
          dist.record((t * samplesPerThread + i).toLong)
        }
      }
    }

    Await.result(Future.sequence(futures), 30.seconds)

    val snap = dist.snapshot()
    snap.count shouldBe threadCount * samplesPerThread
  }
}

