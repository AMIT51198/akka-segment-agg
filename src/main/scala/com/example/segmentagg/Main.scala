package com.example.segmentagg

import java.nio.file.Paths
import scala.annotation.tailrec

/** CLI entry point. Parses arguments and runs the pipeline. */
object Main {

  def main(args: Array[String]): Unit = {
    val config = parseConfig(args.toList)
    AggregationPipeline.run(config)
  }

  private def parseConfig(args: List[String]): PipelineConfig = {
    val parsed          = parseArgs(args, Map.empty)
    val input           = parsed.getOrElse("input", fail("Missing --input"))
    val output          = parsed.getOrElse("output", fail("Missing --output"))
    val defaultP        = parsed.get("parallelism").map(_.toInt)
                                .getOrElse(Runtime.getRuntime.availableProcessors())
    val readParallelism = parsed.get("read-parallelism").map(_.toInt).getOrElse(defaultP)
    val aggParallelism  = parsed.get("aggregate-parallelism").map(_.toInt).getOrElse(defaultP)

    PipelineConfig(
      input               = Paths.get(input),
      output              = Paths.get(output),
      readParallelism     = readParallelism,
      aggregateParallelism = aggParallelism
    )
  }

  @tailrec
  private def parseArgs(args: List[String], acc: Map[String, String]): Map[String, String] = args match {
    case Nil                                       => acc
    case "--input" :: v :: tail                     => parseArgs(tail, acc.updated("input", v))
    case "--output" :: v :: tail                    => parseArgs(tail, acc.updated("output", v))
    case "--parallelism" :: v :: tail               => parseArgs(tail, acc.updated("parallelism", v))
    case "--read-parallelism" :: v :: tail          => parseArgs(tail, acc.updated("read-parallelism", v))
    case "--aggregate-parallelism" :: v :: tail     => parseArgs(tail, acc.updated("aggregate-parallelism", v))
    case unknown :: _                              => fail(s"Unknown argument: $unknown")
  }

  private def fail(msg: String): Nothing = throw new IllegalArgumentException(msg)
}

