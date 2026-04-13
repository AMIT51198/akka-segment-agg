package com.example.segmentagg

import java.time.Instant
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardOpenOption}

import akka.NotUsed
import akka.stream.scaladsl.Source
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.{ArrowFileReader, ArrowReader, ArrowStreamReader, SeekableReadChannel}

object ArrowBatchSource {

  def source(path: Path): Source[ColumnBatch, NotUsed] = {
    Source
      .unfoldResource[ColumnBatch, Cursor](
        create = () => Cursor.open(path),
        read = _.nextBatch(),
        close = _.close()
      )
  }

  final class Cursor private (
      channel: SeekableByteChannel,
      allocator: RootAllocator,
      reader: ArrowReader,
      path: Path
  ) {
    private val BatchLogInterval = 100
    private var batchIndex = 0
    private var firstScanLogged = false
    private var totalScanNanos = 0L
    private var totalMaterializeNanos = 0L

    def nextBatch(): Option[ColumnBatch] = {
      if (!firstScanLogged) {
        firstScanLogged = true
        println(s"[ArrowBatchSource] First scan starting at ${Instant.now()} for $path")
      }

      val scanStart = System.nanoTime()
      val hasBatch = reader.loadNextBatch()
      val scanNanos = System.nanoTime() - scanStart
      totalScanNanos += scanNanos

      if (!hasBatch) {
        println(
          s"[ArrowBatchSource] Reached end of input after $batchIndex batches from $path " +
            s"(total scan=${formatMillis(totalScanNanos)}, total materialize=${formatMillis(totalMaterializeNanos)})"
        )
        None
      } else {
        val root = reader.getVectorSchemaRoot
        val rowCount = root.getRowCount
        batchIndex += 1
        val materializeStart = System.nanoTime()
        val batch =
          if (rowCount == 0) {
            ColumnBatch.empty.copy(batchId = batchIndex)
          } else {
            copyBatch(root, rowCount)
          }
        val materializeNanos = System.nanoTime() - materializeStart
        totalMaterializeNanos += materializeNanos

        if (batchIndex <= 5 || batchIndex % BatchLogInterval == 0 || rowCount == 0) {
          println(
            s"[ArrowBatchSource] Loaded batch #$batchIndex with $rowCount rows from $path " +
              s"(scan=${formatMillis(scanNanos)}, materialize=${formatMillis(materializeNanos)})"
          )
        }

        Some(batch)
      }
    }

    def close(): Unit = {
      println(s"[ArrowBatchSource] Closing input resources for $path")
      reader.close()
      allocator.close()
      channel.close()
    }

    private def copyBatch(root: VectorSchemaRoot, rowCount: Int): ColumnBatch = {
      val segmentVector = root.getVector("segment_id")
      val impressionsVector = root.getVector("impressions")
      val revenueVector = root.getVector("revenue")

      require(segmentVector != null, "Missing Arrow column: segment_id")
      require(impressionsVector != null, "Missing Arrow column: impressions")
      require(revenueVector != null, "Missing Arrow column: revenue")

      val segmentIds = new Array[Long](rowCount)
      val impressions = new Array[Long](rowCount)
      val revenues = new Array[Double](rowCount)

      var i = 0
      while (i < rowCount) {
        if (segmentVector.isNull(i) || impressionsVector.isNull(i) || revenueVector.isNull(i)) {
          throw new IllegalArgumentException(s"Nulls are not supported in the hot path, found at row index $i")
        }
        segmentIds(i) = readLong(segmentVector, i)
        impressions(i) = readLong(impressionsVector, i)
        revenues(i) = readDouble(revenueVector, i)
        i += 1
      }

      ColumnBatch(segmentIds, impressions, revenues, rowCount, batchIndex, 0L, 0L)
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

    private def formatMillis(nanos: Long): String = f"${nanos / 1000000.0}%.2f ms"
  }

  object Cursor {
    def open(path: Path): Cursor = {
      println(s"[ArrowBatchSource] Opening Arrow input $path")
      val channel = Files.newByteChannel(path, StandardOpenOption.READ)
      val allocator = new RootAllocator(Long.MaxValue)
      val reader = createReader(path, channel, allocator)
      new Cursor(channel, allocator, reader, path)
    }

    private def createReader(path: Path, channel: SeekableByteChannel, allocator: RootAllocator): ArrowReader = {
      if (hasArrowFileMagic(channel)) {
        println(s"[ArrowBatchSource] Detected Arrow file container format for $path")
        new ArrowFileReader(new SeekableReadChannel(channel), allocator)
      } else {
        println(s"[ArrowBatchSource] Detected Arrow stream format for $path")
        new ArrowStreamReader(channel, allocator)
      }
    }

    private def hasArrowFileMagic(channel: SeekableByteChannel): Boolean = {
      val expected = "ARROW1".getBytes(StandardCharsets.US_ASCII)
      val header = ByteBuffer.allocate(expected.length)
      channel.position(0L)
      val bytesRead = channel.read(header)
      channel.position(0L)

      if (bytesRead != expected.length) {
        false
      } else {
        header.flip()
        val actual = new Array[Byte](expected.length)
        header.get(actual)
        java.util.Arrays.equals(actual, expected)
      }
    }
  }
}
