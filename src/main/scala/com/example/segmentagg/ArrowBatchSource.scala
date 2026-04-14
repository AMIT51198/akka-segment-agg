package com.example.segmentagg

import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardOpenOption}
import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.Source

import com.example.segmentagg.logging.PipelineLogger
import com.example.segmentagg.metrics.LatencyDistribution
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.{ArrowFileReader, ArrowReader, ArrowStreamReader, SeekableReadChannel}

/**
 * Akka Streams source that reads columnar batches from an Arrow IPC file/stream.
 *
 * Design notes:
 *  - Uses [[PipelineLogger]] for structured logging (Dependency Inversion).
 *  - The internal [[Cursor]] encapsulates all file I/O and vector access,
 *    keeping the public API minimal (Interface Segregation).
 */
object ArrowBatchSource {

  /** Create a source that emits one [[ColumnBatch]] per Arrow record batch. */
  def source(path: Path, logger: PipelineLogger): Source[ColumnBatch, NotUsed] =
    Source.unfoldResource[ColumnBatch, Cursor](
      create = () => Cursor.open(path, logger),
      read   = _.nextBatch(),
      close  = _.close()
    )

  // -- internal cursor --

  private final class Cursor(
      channel: SeekableByteChannel,
      allocator: RootAllocator,
      reader: ArrowReader,
      path: Path,
      logger: PipelineLogger
  ) {
    private val BatchLogInterval      = 100
    private var batchIndex            = 0
    private var firstScanLogged       = false
    private var totalScanNanos        = 0L
    private var totalMaterializeNanos = 0L

    def nextBatch(): Option[ColumnBatch] = {
      if (!firstScanLogged) {
        firstScanLogged = true
        logger.info(s"First scan starting at ${Instant.now()} for $path")
      }

      val scanStart = System.nanoTime()
      val hasBatch  = reader.loadNextBatch()
      val scanNanos = System.nanoTime() - scanStart
      totalScanNanos += scanNanos

      if (!hasBatch) {
        logger.info(
          s"Reached end of input after $batchIndex batches from $path " +
            s"(total scan=${LatencyDistribution.formatMillis(totalScanNanos)}, " +
            s"total materialize=${LatencyDistribution.formatMillis(totalMaterializeNanos)})"
        )
        None
      } else {
        val root     = reader.getVectorSchemaRoot
        val rowCount = root.getRowCount
        batchIndex += 1

        val materializeStart = System.nanoTime()
        val batch =
          if (rowCount == 0) ColumnBatch.Empty.copy(batchId = batchIndex)
          else copyBatch(root, rowCount)
        val materializeNanos = System.nanoTime() - materializeStart
        totalMaterializeNanos += materializeNanos

        if (batchIndex <= 5 || batchIndex % BatchLogInterval == 0 || rowCount == 0) {
          logger.info(
            s"Loaded batch #$batchIndex with $rowCount rows from $path " +
              s"(scan=${LatencyDistribution.formatMillis(scanNanos)}, " +
              s"materialize=${LatencyDistribution.formatMillis(materializeNanos)})"
          )
        }
        Some(batch)
      }
    }

    def close(): Unit = {
      logger.info(s"Closing input resources for $path")
      reader.close()
      allocator.close()
      channel.close()
    }

    // -- batch materialisation --

    private def copyBatch(root: VectorSchemaRoot, rowCount: Int): ColumnBatch = {
      val segmentVector     = requireVector(root, "segment_id")
      val impressionsVector = requireVector(root, "impressions")
      val revenueVector     = requireVector(root, "revenue")

      val segmentIds  = new Array[Long](rowCount)
      val impressions = new Array[Long](rowCount)
      val revenues    = new Array[Double](rowCount)

      var i = 0
      while (i < rowCount) {
        if (segmentVector.isNull(i) || impressionsVector.isNull(i) || revenueVector.isNull(i))
          throw new IllegalArgumentException(s"Nulls are not supported in the hot path, found at row index $i")
        segmentIds(i)  = readLong(segmentVector, i)
        impressions(i) = readLong(impressionsVector, i)
        revenues(i)    = readDouble(revenueVector, i)
        i += 1
      }

      ColumnBatch(segmentIds, impressions, revenues, rowCount, batchIndex, BatchTiming.Zero)
    }

    private def requireVector(root: VectorSchemaRoot, name: String): FieldVector = {
      val vec = root.getVector(name)
      require(vec != null, s"Missing Arrow column: $name")
      vec
    }

    private def readLong(vector: FieldVector, index: Int): Long = vector match {
      case v: BigIntVector   => v.get(index)
      case v: IntVector      => v.get(index).toLong
      case v: SmallIntVector => v.get(index).toLong
      case v: TinyIntVector  => v.get(index).toLong
      case v: UInt1Vector    => v.get(index).toLong
      case v: UInt2Vector    => v.get(index).toLong
      case v: UInt4Vector    => v.get(index).toLong
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported Arrow integral vector type for ${vector.getName}: ${vector.getClass.getName}"
        )
    }

    private def readDouble(vector: FieldVector, index: Int): Double = vector match {
      case v: Float8Vector  => v.get(index)
      case v: Float4Vector  => v.get(index).toDouble
      case v: DecimalVector => v.getObject(index).doubleValue()
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported Arrow floating vector type for ${vector.getName}: ${vector.getClass.getName}"
        )
    }
  }

  private object Cursor {
    def open(path: Path, logger: PipelineLogger): Cursor = {
      logger.info(s"Opening Arrow input $path")
      val channel   = Files.newByteChannel(path, StandardOpenOption.READ)
      val allocator = new RootAllocator(Long.MaxValue)
      val reader    = createReader(path, channel, allocator, logger)
      new Cursor(channel, allocator, reader, path, logger)
    }

    private def createReader(
        path: Path,
        channel: SeekableByteChannel,
        allocator: RootAllocator,
        logger: PipelineLogger
    ): ArrowReader =
      if (hasArrowFileMagic(channel)) {
        logger.info(s"Detected Arrow file container format for $path")
        new ArrowFileReader(new SeekableReadChannel(channel), allocator)
      } else {
        logger.info(s"Detected Arrow stream format for $path")
        new ArrowStreamReader(channel, allocator)
      }

    private def hasArrowFileMagic(channel: SeekableByteChannel): Boolean = {
      val expected = "ARROW1".getBytes(StandardCharsets.US_ASCII)
      val header   = ByteBuffer.allocate(expected.length)
      channel.position(0L)
      val bytesRead = channel.read(header)
      channel.position(0L)

      if (bytesRead != expected.length) false
      else {
        header.flip()
        val actual = new Array[Byte](expected.length)
        header.get(actual)
        java.util.Arrays.equals(actual, expected)
      }
    }
  }
}

