package com.example.segmentagg.model

import java.nio.file.Path

import com.example.segmentagg.parquet.DecodeMode

/**
 * Immutable configuration for the aggregation pipeline.
 *
 * Shared between CLI argument parsing, pipeline orchestration, and
 * individual stages without any of them being coupled to each other.
 */
final case class PipelineConfig(
    input: Path,
    output: Path,
    readParallelism: Int,
    aggregateParallelism: Int,
    decodeMode: DecodeMode = DecodeMode.Scalar
) {

  require(readParallelism > 0, s"readParallelism must be positive, got $readParallelism")
  require(aggregateParallelism > 0, s"aggregateParallelism must be positive, got $aggregateParallelism")

  override def toString: String =
    s"PipelineConfig(input=$input, output=$output, " +
      s"readParallelism=$readParallelism, aggregateParallelism=$aggregateParallelism, " +
      s"decodeMode=$decodeMode)"
}

