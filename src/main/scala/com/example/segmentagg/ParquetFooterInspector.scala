package com.example.segmentagg

import java.nio.file.Paths

import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.ParquetFileReader

object ParquetFooterInspector {
  def main(args: Array[String]): Unit = {
    val input = args.headOption.getOrElse {
      throw new IllegalArgumentException("Usage: runMain com.example.segmentagg.ParquetFooterInspector /path/to/file.parquet")
    }

    val path = new org.apache.hadoop.fs.Path(Paths.get(input).toUri)
    val conf = new Configuration(false)
    val reader = ParquetFileReader.open(conf, path)

    try {
      val footer = reader.getFooter
      val fileMeta = footer.getFileMetaData
      println(s"File: $input")
      println(s"Created by: ${fileMeta.getCreatedBy}")
      println(s"Schema:")
      println(fileMeta.getSchema)
      println(s"Row groups: ${footer.getBlocks.size}")
      println(s"Key/value metadata: ${fileMeta.getKeyValueMetaData}")

      footer.getBlocks.asScala.zipWithIndex.foreach { case (block, index) =>
        println(s"\nRow group #$index")
        println(s"  rowCount=${block.getRowCount}")
        println(s"  totalByteSize=${block.getTotalByteSize}")
        println(s"  rowIndexOffset=${block.getRowIndexOffset}")
        println(s"  compressedSize=${block.getCompressedSize}")
        println(s"  startingPos=${block.getStartingPos}")

        block.getColumns.asScala.foreach { column =>
          println(
            s"  column=${column.getPath.toDotString} " +
              s"type=${column.getPrimitiveType.getPrimitiveTypeName} " +
              s"valueCount=${column.getValueCount} " +
              s"firstDataPageOffset=${column.getFirstDataPageOffset} " +
              s"dictionaryPageOffset=${column.getDictionaryPageOffset} " +
              s"totalSize=${column.getTotalSize}"
          )
        }
      }
    } finally {
      reader.close()
    }
  }
}
