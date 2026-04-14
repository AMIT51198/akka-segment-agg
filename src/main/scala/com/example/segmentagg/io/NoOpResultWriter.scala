package com.example.segmentagg.io

import com.example.segmentagg.model.AggregateRow

/**
 * A no-op [[ResultWriter]] that discards all output.
 *
 * Used in benchmarks and dry-runs where writing results to disk
 * would skew timing measurements.
 */
object NoOpResultWriter extends ResultWriter {
  override def write(rows: Seq[AggregateRow]): Unit = ()
}

