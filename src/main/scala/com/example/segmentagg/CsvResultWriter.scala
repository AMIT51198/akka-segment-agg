package com.example.segmentagg

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardOpenOption}

object CsvResultWriter {
  def write(path: Path, rows: Seq[AggregateRow]): Unit = {
    val parent = path.getParent
    if (parent != null) {
      Files.createDirectories(parent)
    }

    println(s"[CsvResultWriter] Writing ${rows.size} rows to $path")
    val writer = Files.newBufferedWriter(
      path,
      StandardCharsets.UTF_8,
      StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING,
      StandardOpenOption.WRITE
    )

    try {
      writer.write("segment_id,total_impressions,total_revenue")
      writer.newLine()

      rows.foreach { row =>
        writer.write(s"${row.segmentId},${row.totalImpressions},${row.totalRevenue}")
        writer.newLine()
      }
    } finally {
      writer.close()
      println(s"[CsvResultWriter] Finished writing $path")
    }
  }
}
