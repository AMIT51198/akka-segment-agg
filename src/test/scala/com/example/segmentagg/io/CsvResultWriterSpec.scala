package com.example.segmentagg.io

import java.nio.file.{Files, Path}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.example.segmentagg.logging.PipelineLogger
import com.example.segmentagg.model.AggregateRow

/**
 * Tests for [[CsvResultWriter]].
 */
class CsvResultWriterSpec extends AnyFunSuite with Matchers {

  private val silentLogger: PipelineLogger = new PipelineLogger {
    override def info(message: String): Unit = ()
    override def warn(message: String): Unit = ()
    override def error(message: String, cause: Option[Throwable]): Unit = ()
  }

  private def withTempFile(testFn: Path => Unit): Unit = {
    val tmp = Files.createTempFile("csv-test-", ".csv")
    try testFn(tmp)
    finally Files.deleteIfExists(tmp)
  }

  test("writes header and rows correctly") {
    withTempFile { path =>
      val rows = Seq(
        AggregateRow(1L, 100L, 10.5),
        AggregateRow(2L, 200L, 20.25),
        AggregateRow(3L, 300L, 30.0)
      )

      new CsvResultWriter(path, silentLogger).write(rows)

      val lines = Files.readAllLines(path)
      lines.size() shouldBe 4
      lines.get(0) shouldBe "segment_id,total_impressions,total_revenue"
      lines.get(1) shouldBe "1,100,10.5"
      lines.get(2) shouldBe "2,200,20.25"
      lines.get(3) shouldBe "3,300,30.0"
    }
  }

  test("writes empty result set with header only") {
    withTempFile { path =>
      new CsvResultWriter(path, silentLogger).write(Seq.empty)

      val lines = Files.readAllLines(path)
      lines.size() shouldBe 1
      lines.get(0) shouldBe "segment_id,total_impressions,total_revenue"
    }
  }

  test("creates parent directories if they don't exist") {
    val tmp = Files.createTempDirectory("csv-test-dir-")
    val nested = tmp.resolve("a/b/c/output.csv")
    try {
      new CsvResultWriter(nested, silentLogger).write(Seq(AggregateRow(1L, 1L, 1.0)))
      Files.exists(nested) shouldBe true
    } finally {
      // Cleanup
      Files.deleteIfExists(nested)
      Files.deleteIfExists(nested.getParent)
      Files.deleteIfExists(nested.getParent.getParent)
      Files.deleteIfExists(nested.getParent.getParent.getParent)
      Files.deleteIfExists(tmp)
    }
  }

  test("overwrites existing file") {
    withTempFile { path =>
      // Write first time
      new CsvResultWriter(path, silentLogger).write(Seq(AggregateRow(1L, 1L, 1.0)))
      // Overwrite with different data
      new CsvResultWriter(path, silentLogger).write(Seq(AggregateRow(99L, 99L, 99.0)))

      val lines = Files.readAllLines(path)
      lines.size() shouldBe 2
      lines.get(1) shouldBe "99,99,99.0"
    }
  }
}

