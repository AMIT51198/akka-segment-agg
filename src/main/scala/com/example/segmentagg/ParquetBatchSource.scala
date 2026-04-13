package com.example.segmentagg

import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._
import scala.util.Using

import akka.NotUsed
import akka.stream.Attributes
import akka.stream.scaladsl.Source
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.HadoopReadOptions
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.{MessageType, Type}

import com.example.segmentagg.parquet.RequiredColumnDecoders

object ParquetBatchSource {
  private val RequiredColumns = List("segment_id", "impressions", "revenue")

  final case class FileReadPlan(
      path: Path,
      footer: ParquetMetadata,
      schema: MessageType,
      decoders: RequiredColumnDecoders,
      rowGroupCount: Int,
      batchIdStart: Int
  )

  def source(input: Path, readParallelism: Int): Source[ColumnBatch, NotUsed] = {
    val conf = new Configuration(false)
    val files = resolveInputFiles(input)
    val nextBatchId = new AtomicInteger(0)
    val readPlans = files.map(path => buildReadPlan(path, conf, nextBatchId))
    val rowGroupCount = readPlans.map(_.rowGroupCount).sum

    println(
      s"[ParquetBatchSource] Resolved ${files.size} parquet files and $rowGroupCount row groups from $input"
    )

    Source(readPlans)
      .addAttributes(Attributes.inputBuffer(initial = 1, max = 1))
      .flatMapMerge(
        breadth = math.max(1, math.min(readParallelism, readPlans.size)),
        f = plan => rowGroupSource(plan, conf)
      )
      .addAttributes(Attributes.inputBuffer(initial = 1, max = 1))
  }

  private def buildReadPlan(path: Path, conf: Configuration, nextBatchId: AtomicInteger): FileReadPlan = {
    val inputFile = HadoopInputFile.fromPath(hadoopPath(path), conf)
    val footer = Using.resource(ParquetFileReader.open(inputFile, readOptions(conf, path)))(_.getFooter)
    val schema = projectedSchema(footer.getFileMetaData.getSchema)
    val decoders = RequiredColumnDecoders.build(footer, schema)
    val rowGroupCount = footer.getBlocks.size
    val batchIdStart = nextBatchId.getAndAdd(rowGroupCount)
    FileReadPlan(path, footer, schema, decoders, rowGroupCount, batchIdStart)
  }

  private def projectedSchema(fileSchema: MessageType): MessageType =
    new MessageType(fileSchema.getName, RequiredColumns.map(requiredField(fileSchema, _)).asJava)

  private def requiredField(fileSchema: MessageType, name: String): Type =
    fileSchema.getFields.asScala
      .find(_.getName == name)
      .getOrElse(throw new IllegalArgumentException(s"Missing Parquet column: $name"))

  private def hadoopPath(path: Path): org.apache.hadoop.fs.Path =
    new org.apache.hadoop.fs.Path(path.toUri)

  private def readOptions(conf: Configuration, path: Path) =
    HadoopReadOptions.builder(conf, hadoopPath(path)).build()

  private def resolveInputFiles(input: Path): List[Path] = {
    if (Files.isRegularFile(input)) {
      List(input)
    } else if (Files.isDirectory(input)) {
      Using.resource(Files.walk(input)) { stream =>
        stream.iterator().asScala
          .filter(path => Files.isRegularFile(path) && path.getFileName.toString.toLowerCase.endsWith(".parquet"))
          .toList
          .sorted
      }
    } else {
      throw new IllegalArgumentException(s"Input path does not exist or is unsupported: $input")
    }
  }

  class RowGroupCursor private (
      file: FileReadPlan,
      reader: ParquetFileReader
  ) {
    private var nextRowGroupIndex = 0

    def nextBatch(): Option[ColumnBatch] = {
      if (nextRowGroupIndex >= file.rowGroupCount) {
        None
      } else {
        val rowGroupIndex = nextRowGroupIndex
        nextRowGroupIndex += 1
        Some(readRowGroup(rowGroupIndex))
      }
    }

    def close(): Unit = {
      reader.close()
    }

    private def readRowGroup(rowGroupIndex: Int): ColumnBatch = {
      val readStartNanos = System.nanoTime()
      val pages = reader.readRowGroup(rowGroupIndex)
      if (pages == null) {
        throw new IllegalStateException(
          s"Parquet row group $rowGroupIndex could not be read from ${file.path}"
        )
      }

      try {
        val rowCount = pages.getRowCount.toInt
        val segmentIds = new Array[Long](rowCount)
        val impressions = new Array[Long](rowCount)
        val revenues = new Array[Double](rowCount)

        file.decoders.segmentIds.decode(pages, segmentIds, rowCount)
        file.decoders.impressions.decode(pages, impressions, rowCount)
        file.decoders.revenues.decode(pages, revenues, rowCount)

        val readEndNanos = System.nanoTime()
        val batchId = file.batchIdStart + rowGroupIndex + 1
        ColumnBatch(segmentIds, impressions, revenues, rowCount, batchId, readStartNanos, readEndNanos)
      } finally {
        pages.close()
      }
    }
  }

  private def rowGroupSource(file: FileReadPlan, conf: Configuration): Source[ColumnBatch, NotUsed] = {
    Source
      .unfoldResource[ColumnBatch, RowGroupCursor](
        create = () => RowGroupCursor.open(file, conf),
        read = _.nextBatch(),
        close = _.close()
      )
      .addAttributes(Attributes.inputBuffer(initial = 1, max = 1))
  }

  private object RowGroupCursor {
    def open(file: FileReadPlan, conf: Configuration): RowGroupCursor = {
      val inputFile = HadoopInputFile.fromPath(hadoopPath(file.path), conf)
      val reader = ParquetFileReader.open(inputFile, readOptions(conf, file.path))
      reader.setRequestedSchema(file.schema)

      new RowGroupCursor(file, reader)
    }
  }
}
