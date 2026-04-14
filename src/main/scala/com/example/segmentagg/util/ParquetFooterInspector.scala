package com.example.segmentagg.util

import java.nio.file.Paths

import scala.jdk.CollectionConverters._
import scala.util.Using

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile

import com.example.segmentagg.logging.PipelineLogger

/**
 * Standalone utility that prints the Parquet footer metadata for a single file.
 *
 * Usage: sbt "runMain com.example.segmentagg.util.ParquetFooterInspector /path/to/file.parquet"
 */
object ParquetFooterInspector {

  def main(args: Array[String]): Unit = {
    val input = args.headOption.getOrElse {
      throw new IllegalArgumentException(
        "Usage: runMain com.example.segmentagg.util.ParquetFooterInspector /path/to/file.parquet"
      )
    }
    inspect(input, PipelineLogger.console("ParquetFooterInspector"))
  }

  def inspect(input: String, logger: PipelineLogger): Unit = {
    val path = new org.apache.hadoop.fs.Path(Paths.get(input).toUri)
    val conf = new Configuration(false)

    Using.resource(ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) { reader =>
      val footer   = reader.getFooter
      val fileMeta = footer.getFileMetaData

      logger.info(s"File: $input")
      logger.info(s"Created by: ${fileMeta.getCreatedBy}")
      logger.info(s"Schema:\n${fileMeta.getSchema}")
      logger.info(s"Row groups: ${footer.getBlocks.size}")
      logger.info(s"Key/value metadata: ${fileMeta.getKeyValueMetaData}")

      footer.getBlocks.asScala.zipWithIndex.foreach { case (block, index) =>
        logger.info(s"\nRow group #$index")
        logger.info(s"  rowCount=${block.getRowCount}")
        logger.info(s"  totalByteSize=${block.getTotalByteSize}")
        logger.info(s"  rowIndexOffset=${block.getRowIndexOffset}")
        logger.info(s"  compressedSize=${block.getCompressedSize}")
        logger.info(s"  startingPos=${block.getStartingPos}")

        block.getColumns.asScala.foreach { column =>
          logger.info(
            s"  column=${column.getPath.toDotString} " +
              s"type=${column.getPrimitiveType.getPrimitiveTypeName} " +
              s"valueCount=${column.getValueCount} " +
              s"firstDataPageOffset=${column.getFirstDataPageOffset} " +
              s"dictionaryPageOffset=${column.getDictionaryPageOffset} " +
              s"totalSize=${column.getTotalSize}"
          )
        }
      }
    }
  }
}

