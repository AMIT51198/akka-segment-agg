package com.example.segmentagg.io

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardOpenOption}

import scala.util.Using

import com.example.segmentagg.logging.PipelineLogger
import com.example.segmentagg.model.AggregateRow

/**
 * Writes aggregation results as a CSV file.
 *
 * Creates parent directories if they do not exist, truncates any existing
 * file at the given path, and writes a header row followed by one row per
 * [[AggregateRow]].
 */
final class CsvResultWriter(path: Path, logger: PipelineLogger) extends ResultWriter {

  override def write(rows: Seq[AggregateRow]): Unit = {
    Option(path.getParent).foreach(Files.createDirectories(_))

    logger.info(s"Writing ${rows.size} rows to $path")

    Using.resource(
      Files.newBufferedWriter(
        path,
        StandardCharsets.UTF_8,
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING,
        StandardOpenOption.WRITE
      )
    ) { writer =>
      writer.write("segment_id,total_impressions,total_revenue")
      writer.newLine()
      rows.foreach { row =>
        writer.write(s"${row.segmentId},${row.totalImpressions},${row.totalRevenue}")
        writer.newLine()
      }
    }

    logger.info(s"Finished writing $path")
  }
}

