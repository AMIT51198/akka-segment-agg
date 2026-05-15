package com.example.segmentagg

import java.nio.file.Path

/** Immutable pipeline configuration. */
final case class PipelineConfig(
    input: Path,
    output: Path,
    readParallelism: Int,
    aggregateParallelism: Int
) {
  require(readParallelism > 0, s"readParallelism must be positive, got $readParallelism")
  require(aggregateParallelism > 0, s"aggregateParallelism must be positive, got $aggregateParallelism")
}

