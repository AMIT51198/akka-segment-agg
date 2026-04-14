package com.example.segmentagg.util

import java.io.{File, PrintWriter}
import java.nio.file.Paths

import com.example.segmentagg.AggregationPipeline
import com.example.segmentagg.io.NoOpResultWriter
import com.example.segmentagg.logging.PipelineLogger
import com.example.segmentagg.metrics.StageMetrics
import com.example.segmentagg.model.PipelineConfig
import com.example.segmentagg.parquet.{DecodeMode, DecoderPageMetrics}

/**
 * Runs all combinations of read-parallelism × aggregate-parallelism with vectorised decoding.
 *
 * Results are written to `/tmp/benchmark_results.csv` after every run.
 *
 * Usage: sbt "runMain com.example.segmentagg.util.Benchmark /path/to/parquet/dir"
 */
object Benchmark {

  private val ResultsFile = new File("/tmp/benchmark_results.csv")

  private case class Run(readP: Int, aggP: Int, elapsedMs: Double)

  def main(args: Array[String]): Unit = {
    val input = args.headOption.getOrElse(
      throw new IllegalArgumentException(
        "Usage: runMain com.example.segmentagg.util.Benchmark /path/to/parquet/dir"
      )
    )
    val output = "/tmp/benchmark_dummy.csv"

    val readParallelisms = List(4, 6, 8, 10, 12, 16)
    val aggParallelisms  = List(4, 6, 8, 10, 12)

    val csvWriter = new PrintWriter(ResultsFile)
    csvWriter.println("read_parallelism,aggregate_parallelism,elapsed_ms")
    csvWriter.flush()

    println("=" * 70)
    println("WARMUP RUN (vectorised, read=4, agg=4)")
    println("=" * 70)
    runOnce(input, output, 4, 4)
    println("WARMUP DONE")
    println()

    val results = scala.collection.mutable.ArrayBuffer.empty[Run]

    for (rp <- readParallelisms; ap <- aggParallelisms; if ap <= rp) {
      println("=" * 70)
      println(s"RUN: vectorised  read=$rp  agg=$ap")
      println("=" * 70)

      val elapsedMs = runOnce(input, output, rp, ap)
      results += Run(rp, ap, elapsedMs)

      csvWriter.println(s"$rp,$ap,${f"$elapsedMs%.2f"}")
      csvWriter.flush()

      println(f">>> read=$rp%2d  agg=$ap%2d  elapsed=$elapsedMs%10.2f ms")
      println()
    }
    csvWriter.close()

    println()
    println("=" * 60)
    println("ALL RESULTS (vectorised)")
    println("=" * 60)
    println(f"${"read_p"}%-8s | ${"agg_p"}%-8s | ${"elapsed_ms"}%12s")
    println("-" * 60)
    results.foreach { r =>
      println(f"${r.readP}%-8d | ${r.aggP}%-8d | ${r.elapsedMs}%12.2f ms")
    }

    println()
    println("=" * 60)
    println("TOP 10 FASTEST")
    println("=" * 60)
    println(f"${"#"}%-4s ${"read_p"}%-8s | ${"agg_p"}%-8s | ${"elapsed_ms"}%12s")
    println("-" * 60)
    results.sortBy(_.elapsedMs).take(10).zipWithIndex.foreach { case (r, i) =>
      println(f"${i + 1}%-4d ${r.readP}%-8d | ${r.aggP}%-8d | ${r.elapsedMs}%12.2f ms")
    }

    println()
    println("=" * 60)
    println("BEST aggregate-parallelism PER read-parallelism")
    println("=" * 60)
    results.groupBy(_.readP).toSeq.sortBy(_._1).foreach { case (rp, runs) =>
      val best = runs.minBy(_.elapsedMs)
      println(f"  read=$rp%2d → best agg=${best.aggP}%2d  (${best.elapsedMs}%10.2f ms)")
    }

    println()
    println("=" * 60)
    println("BEST read-parallelism PER aggregate-parallelism")
    println("=" * 60)
    results.groupBy(_.aggP).toSeq.sortBy(_._1).foreach { case (ap, runs) =>
      val best = runs.minBy(_.elapsedMs)
      println(f"  agg=$ap%2d → best read=${best.readP}%2d  (${best.elapsedMs}%10.2f ms)")
    }

    println()
    println(s"SLOWEST: ${results.maxBy(_.elapsedMs)}")
    println(s"FASTEST: ${results.minBy(_.elapsedMs)}")
    println(s"\nResults saved to: ${ResultsFile.getAbsolutePath}")
  }

  private def runOnce(input: String, output: String, readP: Int, aggP: Int): Double = {
    val config = PipelineConfig(
      input                = Paths.get(input),
      output               = Paths.get(output),
      readParallelism      = readP,
      aggregateParallelism = aggP,
      decodeMode           = DecodeMode.Vectorised
    )

    val logger = new PipelineLogger {
      override def info(message: String): Unit                            = ()
      override def warn(message: String): Unit                            = ()
      override def error(message: String, cause: Option[Throwable]): Unit =
        System.err.println(s"[ERROR] $message")
    }

    val stageMetrics = StageMetrics.pipelineDefault
    val pageMetrics  = DecoderPageMetrics.reset()

    val startNanos = System.nanoTime()
    new AggregationPipeline(config, logger, stageMetrics, pageMetrics, NoOpResultWriter).run()
    val elapsedNanos = System.nanoTime() - startNanos
    elapsedNanos / 1e6
  }
}

