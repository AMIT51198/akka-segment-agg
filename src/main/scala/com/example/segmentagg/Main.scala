package com.example.segmentagg

import java.nio.file.Paths

object Main {
  def main(args: Array[String]): Unit = {
    val parsed = parseArgs(args.toList, Map.empty)

    val input = parsed.getOrElse("input", fail("Missing --input /path/to/file.parquet-or-directory"))
    val output = parsed.getOrElse("output", fail("Missing --output /path/to/result.csv"))
    val defaultParallelism = parsed.get("parallelism").map(_.toInt).getOrElse(Runtime.getRuntime.availableProcessors())
    val readParallelism = parsed.get("read-parallelism").map(_.toInt).getOrElse(defaultParallelism)
    val aggregateParallelism = parsed.get("aggregate-parallelism").map(_.toInt).getOrElse(defaultParallelism)

    require(readParallelism > 0, s"read-parallelism must be positive, got $readParallelism")
    require(aggregateParallelism > 0, s"aggregate-parallelism must be positive, got $aggregateParallelism")

    AggregationPipeline.run(
      AggregationPipeline.PipelineConfig(
        input = Paths.get(input),
        output = Paths.get(output),
        readParallelism = readParallelism,
        aggregateParallelism = aggregateParallelism
      )
    )
  }

  private def parseArgs(args: List[String], acc: Map[String, String]): Map[String, String] = args match {
    case Nil => acc
    case "--input" :: value :: tail =>
      parseArgs(tail, acc.updated("input", value))
    case "--output" :: value :: tail =>
      parseArgs(tail, acc.updated("output", value))
    case "--parallelism" :: value :: tail =>
      parseArgs(tail, acc.updated("parallelism", value))
    case "--read-parallelism" :: value :: tail =>
      parseArgs(tail, acc.updated("read-parallelism", value))
    case "--aggregate-parallelism" :: value :: tail =>
      parseArgs(tail, acc.updated("aggregate-parallelism", value))
    case unknown :: _ =>
      fail(s"Unknown or incomplete argument: $unknown")
  }

  private def fail(message: String): Nothing = {
    throw new IllegalArgumentException(message)
  }
}
