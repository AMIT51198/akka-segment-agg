package com.example.segmentagg

import java.nio.file.Paths

import scala.annotation.tailrec

import com.example.segmentagg.model.PipelineConfig
import com.example.segmentagg.parquet.DecodeMode

/**
 * CLI entry point — parses arguments and delegates to [[AggregationPipeline]].
 */
object Main {

  def main(args: Array[String]): Unit = {
    val config = parseConfig(args.toList)
    AggregationPipeline.run(config)
  }

  // -- argument parsing --

  private def parseConfig(args: List[String]): PipelineConfig = {
    val parsed = parseArgs(args, Map.empty)

    val input               = parsed.getOrElse("input", fail("Missing --input /path/to/file.parquet-or-directory"))
    val output              = parsed.getOrElse("output", fail("Missing --output /path/to/result.csv"))
    val defaultParallelism  = parsed.get("parallelism").map(_.toInt).getOrElse(Runtime.getRuntime.availableProcessors())
    val readParallelism     = parsed.get("read-parallelism").map(_.toInt).getOrElse(defaultParallelism)
    val aggregateParallelism = parsed.get("aggregate-parallelism").map(_.toInt).getOrElse(defaultParallelism)
    val decodeMode          = parsed.get("decode-mode").map(DecodeMode.fromString).getOrElse(DecodeMode.Scalar)

    PipelineConfig(
      input                = Paths.get(input),
      output               = Paths.get(output),
      readParallelism      = readParallelism,
      aggregateParallelism = aggregateParallelism,
      decodeMode           = decodeMode
    )
  }

  @tailrec
  private def parseArgs(args: List[String], acc: Map[String, String]): Map[String, String] = args match {
    case Nil                                       => acc
    case "--input" :: value :: tail                 => parseArgs(tail, acc.updated("input", value))
    case "--output" :: value :: tail                => parseArgs(tail, acc.updated("output", value))
    case "--parallelism" :: value :: tail           => parseArgs(tail, acc.updated("parallelism", value))
    case "--read-parallelism" :: value :: tail      => parseArgs(tail, acc.updated("read-parallelism", value))
    case "--aggregate-parallelism" :: value :: tail => parseArgs(tail, acc.updated("aggregate-parallelism", value))
    case "--decode-mode" :: value :: tail           => parseArgs(tail, acc.updated("decode-mode", value))
    case unknown :: _                              => fail(s"Unknown or incomplete argument: $unknown")
  }

  private def fail(message: String): Nothing =
    throw new IllegalArgumentException(message)
}

