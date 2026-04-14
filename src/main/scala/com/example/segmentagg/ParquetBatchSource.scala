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

import com.example.segmentagg.logging.PipelineLogger
import com.example.segmentagg.parquet.{DecodeMode, RequiredColumnDecoders}

/**
 * Akka Streams source that reads columnar batches from Parquet files.
 *
 * Resolves all row groups from the input path and emits one [[ColumnBatch]]
 * per row group. Decode strategy is plugged in via [[DecodeMode]].
 */
object ParquetBatchSource {

  private val RequiredColumns: List[String] = List("segment_id", "impressions", "revenue")

  /**
   * The read plan for a single Parquet file — everything the row-group cursor
   * needs to decode batches without re-reading the footer.
   */
  private final case class FileReadPlan(
      path: Path,
      footer: ParquetMetadata,
      schema: MessageType,
      decoders: RequiredColumnDecoders,
      rowGroupCount: Int,
      batchIdStart: Int
  )

  /** Build a [[Source]] that emits one [[ColumnBatch]] per Parquet row group. */
  def source(
      input: Path,
      readParallelism: Int,
      decodeMode: DecodeMode = DecodeMode.Scalar,
      logger: PipelineLogger = PipelineLogger.console("ParquetBatchSource")
  ): Source[ColumnBatch, NotUsed] = {
    val conf      = new Configuration(false)
    val files     = resolveInputFiles(input)
    val nextBatch = new AtomicInteger(0)
    val plans     = files.map(buildReadPlan(_, conf, nextBatch, decodeMode))
    val rgCount   = plans.map(_.rowGroupCount).sum

    logger.info(s"Resolved ${files.size} parquet files and $rgCount row groups from $input")

    Source(plans)
      .addAttributes(Attributes.inputBuffer(initial = 1, max = 1))
      .flatMapMerge(
        breadth = math.max(1, math.min(readParallelism, plans.size)),
        f = plan => rowGroupSource(plan, conf)
      )
      .addAttributes(Attributes.inputBuffer(initial = 1, max = 1))
  }

  // -- read plan construction --

  private def buildReadPlan(
      path: Path,
      conf: Configuration,
      nextBatchId: AtomicInteger,
      decodeMode: DecodeMode
  ): FileReadPlan = {
    val inputFile    = HadoopInputFile.fromPath(hadoopPath(path), conf)
    val footer       = Using.resource(ParquetFileReader.open(inputFile, readOptions(conf, path)))(_.getFooter)
    val schema       = projectedSchema(footer.getFileMetaData.getSchema)
    val decoders     = RequiredColumnDecoders.build(footer, schema, decodeMode)
    val rowGroupCnt  = footer.getBlocks.size
    val batchIdStart = nextBatchId.getAndAdd(rowGroupCnt)
    FileReadPlan(path, footer, schema, decoders, rowGroupCnt, batchIdStart)
  }

  // -- schema helpers --

  private def projectedSchema(fileSchema: MessageType): MessageType =
    new MessageType(fileSchema.getName, RequiredColumns.map(requiredField(fileSchema, _)).asJava)

  private def requiredField(fileSchema: MessageType, name: String): Type =
    fileSchema.getFields.asScala
      .find(_.getName == name)
      .getOrElse(throw new IllegalArgumentException(s"Missing Parquet column: $name"))

  // -- Hadoop path helpers --

  private def hadoopPath(path: Path): org.apache.hadoop.fs.Path =
    new org.apache.hadoop.fs.Path(path.toUri)

  private def readOptions(conf: Configuration, path: Path) =
    HadoopReadOptions.builder(conf, hadoopPath(path)).build()

  // -- file resolution --

  private def resolveInputFiles(input: Path): List[Path] =
    if (Files.isRegularFile(input)) {
      List(input)
    } else if (Files.isDirectory(input)) {
      Using.resource(Files.walk(input)) { stream =>
        stream.iterator().asScala
          .filter(p => Files.isRegularFile(p) && p.getFileName.toString.toLowerCase.endsWith(".parquet"))
          .toList
          .sorted
      }
    } else {
      throw new IllegalArgumentException(s"Input path does not exist or is unsupported: $input")
    }

  // -- row-group cursor (encapsulates mutable reader state) --

  private final class RowGroupCursor(file: FileReadPlan, reader: ParquetFileReader) {
    private var nextIndex = 0

    def nextBatch(): Option[ColumnBatch] =
      if (nextIndex >= file.rowGroupCount) None
      else {
        val idx = nextIndex
        nextIndex += 1
        Some(readRowGroup(idx))
      }

    def close(): Unit = reader.close()

    private def readRowGroup(rowGroupIndex: Int): ColumnBatch = {
      val fetchStart = System.nanoTime()
      val pages      = reader.readRowGroup(rowGroupIndex)
      val fetchEnd   = System.nanoTime()

      if (pages == null)
        throw new IllegalStateException(s"Parquet row group $rowGroupIndex could not be read from ${file.path}")

      try {
        val rowCount    = pages.getRowCount.toInt
        val segmentIds  = new Array[Long](rowCount)
        val impressions = new Array[Long](rowCount)
        val revenues    = new Array[Double](rowCount)

        file.decoders.segmentIds.decode(pages, segmentIds, rowCount)
        val segDecodeEnd = System.nanoTime()

        file.decoders.impressions.decode(pages, impressions, rowCount)
        val impDecodeEnd = System.nanoTime()

        file.decoders.revenues.decode(pages, revenues, rowCount)
        val decodeEnd = System.nanoTime()

        ColumnBatch(
          segmentIds, impressions, revenues,
          size    = rowCount,
          batchId = file.batchIdStart + rowGroupIndex + 1,
          timing  = BatchTiming(fetchStart, fetchEnd, segDecodeEnd, impDecodeEnd, decodeEnd)
        )
      } finally {
        pages.close()
      }
    }
  }

  private object RowGroupCursor {
    def open(file: FileReadPlan, conf: Configuration): RowGroupCursor = {
      val inputFile = HadoopInputFile.fromPath(hadoopPath(file.path), conf)
      val reader    = ParquetFileReader.open(inputFile, readOptions(conf, file.path))
      reader.setRequestedSchema(file.schema)
      new RowGroupCursor(file, reader)
    }
  }

  private def rowGroupSource(file: FileReadPlan, conf: Configuration): Source[ColumnBatch, NotUsed] =
    Source
      .unfoldResource[ColumnBatch, RowGroupCursor](
        create = () => RowGroupCursor.open(file, conf),
        read   = _.nextBatch(),
        close  = _.close()
      )
      .addAttributes(Attributes.inputBuffer(initial = 1, max = 1))
}

