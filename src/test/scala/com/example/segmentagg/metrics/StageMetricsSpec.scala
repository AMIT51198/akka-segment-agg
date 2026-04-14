package com.example.segmentagg.metrics

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.example.segmentagg.logging.PipelineLogger

/**
 * Tests for [[StageMetrics]].
 */
class StageMetricsSpec extends AnyFunSuite with Matchers {

  test("pipelineDefault exposes all expected stages") {
    val sm = StageMetrics.pipelineDefault
    val stages = List("fetch", "segmentDecode", "impressionDecode", "revenueDecode",
      "decode", "read", "wait", "aggregate")

    stages.foreach { name =>
      noException shouldBe thrownBy(sm(name))
    }
  }

  test("accessing unknown stage throws NoSuchElementException") {
    val sm = StageMetrics.pipelineDefault
    a[NoSuchElementException] shouldBe thrownBy(sm("nonexistent"))
  }

  test("recording and reading back from a stage") {
    val sm = StageMetrics.pipelineDefault
    sm("fetch").record(42L)
    sm("fetch").record(84L)

    val snap = sm("fetch").snapshot()
    snap.count shouldBe 2
    snap.sorted shouldBe IndexedSeq(42L, 84L)
  }

  test("printSummary captures all stage names") {
    val sm = StageMetrics.pipelineDefault
    sm("fetch").record(100L)
    sm("aggregate").record(200L)

    val messages = scala.collection.mutable.ArrayBuffer.empty[String]
    val logger = new PipelineLogger {
      override def info(message: String): Unit = messages += message
      override def warn(message: String): Unit = ()
      override def error(message: String, cause: Option[Throwable]): Unit = ()
    }

    sm.printSummary(logger)

    messages.exists(_.contains("fetch")) shouldBe true
    messages.exists(_.contains("aggregate")) shouldBe true
  }
}

