package com.example.segmentagg

import java.nio.file.{Files, Path}
import java.time.Instant

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

import com.example.segmentagg.parquet.ColumnDecoder

/** Akka Streams source: emits one ColumnBatch per Parquet row group. */
object ParquetBatchSource {

  private val Columns = List("segment_id", "impressions", "revenue")

  private case class FilePlan(
      path: Path,
      schema: MessageType,
      rowGroupCount: Int,
      segDecoder: ColumnDecoder[Long],
      impDecoder: ColumnDecoder[Long],
      revDecoder: ColumnDecoder[Double]
  )

  def source(input: Path, readParallelism: Int): Source[ColumnBatch, NotUsed] = {
    val conf  = new Configuration(false)
    val files = resolveFiles(input)
    val plans = files.map(buildPlan(_, conf))
    val total = plans.map(_.rowGroupCount).sum

    println(s"[${Instant.now()}] [ParquetBatchSource] INFO  Resolved ${files.size} parquet files and $total row groups from $input")

    Source(plans)
      .addAttributes(Attributes.inputBuffer(initial = 1, max = 1))
      .flatMapMerge(
        breadth = math.min(readParallelism, plans.size).max(1),
        plan => rowGroupSource(plan, conf)
      )
      .addAttributes(Attributes.inputBuffer(initial = 1, max = 1))
  }

  private def buildPlan(path: Path, conf: Configuration): FilePlan = {
    val inputFile = HadoopInputFile.fromPath(hadoopPath(path), conf)
    val footer    = Using.resource(ParquetFileReader.open(inputFile, readOpts(conf, path)))(_.getFooter)
    val schema    = projectSchema(footer.getFileMetaData.getSchema)

    FilePlan(
      path         = path,
      schema       = schema,
      rowGroupCount = footer.getBlocks.size,
      segDecoder   = ColumnDecoder.forLong("segment_id", schema),
      impDecoder   = ColumnDecoder.forLong("impressions", schema),
      revDecoder   = ColumnDecoder.forDouble("revenue", schema)
    )
  }

  private def rowGroupSource(plan: FilePlan, conf: Configuration): Source[ColumnBatch, NotUsed] =
    Source.unfoldResource[ColumnBatch, RowGroupCursor](
      create = () => RowGroupCursor.open(plan, conf),
      read   = _.next(),
      close  = _.close()
    ).addAttributes(Attributes.inputBuffer(initial = 1, max = 1))

  private final class RowGroupCursor(plan: FilePlan, reader: ParquetFileReader) {
    private var idx = 0

    def next(): Option[ColumnBatch] = {
      if (idx >= plan.rowGroupCount) return None
      idx += 1

      val fetchStart = System.nanoTime()
      val pages      = reader.readRowGroup(idx - 1)
      val fetchEnd   = System.nanoTime()

      try {
        val rowCount    = pages.getRowCount.toInt
        val segmentIds  = new Array[Long](rowCount)
        val impressions = new Array[Long](rowCount)
        val revenues    = new Array[Double](rowCount)

        plan.segDecoder.decode(pages, segmentIds, rowCount)
        plan.impDecoder.decode(pages, impressions, rowCount)
        plan.revDecoder.decode(pages, revenues, rowCount)
        val decodeEnd = System.nanoTime()

        Some(ColumnBatch(segmentIds, impressions, revenues, rowCount, BatchTiming(fetchStart, fetchEnd, decodeEnd)))
      } finally {
        pages.close()
      }
    }

    def close(): Unit = reader.close()
  }

  private object RowGroupCursor {
    def open(plan: FilePlan, conf: Configuration): RowGroupCursor = {
      val inputFile = HadoopInputFile.fromPath(hadoopPath(plan.path), conf)
      val reader    = ParquetFileReader.open(inputFile, readOpts(conf, plan.path))
      reader.setRequestedSchema(plan.schema)
      new RowGroupCursor(plan, reader)
    }
  }

  private def resolveFiles(input: Path): List[Path] =
    if (Files.isRegularFile(input)) List(input)
    else if (Files.isDirectory(input))
      Using.resource(Files.walk(input)) { stream =>
        stream.iterator().asScala
          .filter(p => Files.isRegularFile(p) && p.toString.toLowerCase.endsWith(".parquet"))
          .toList.sorted
      }
    else throw new IllegalArgumentException(s"Input path does not exist: $input")

  private def projectSchema(fileSchema: MessageType): MessageType =
    new MessageType(fileSchema.getName, Columns.map { name =>
      fileSchema.getFields.asScala.find(_.getName == name)
        .getOrElse(throw new IllegalArgumentException(s"Missing column: $name"))
    }.asJava)

  private def hadoopPath(p: Path) = new org.apache.hadoop.fs.Path(p.toUri)
  private def readOpts(conf: Configuration, p: Path) = HadoopReadOptions.builder(conf, hadoopPath(p)).build()
}

