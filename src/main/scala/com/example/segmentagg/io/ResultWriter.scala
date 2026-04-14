package com.example.segmentagg.io

import com.example.segmentagg.model.AggregateRow

/**
 * Abstraction for writing aggregation results.
 *
 * The pipeline depends on this trait rather than a concrete writer,
 * so different output formats (CSV, no-op for benchmarks, etc.) can
 * be swapped in without touching pipeline code.
 */
trait ResultWriter {

  /** Write the aggregated rows to the configured destination. */
  def write(rows: Seq[AggregateRow]): Unit
}

