package com.example.segmentagg.parquet

import java.io.{DataInputStream, InputStream}
import java.nio.{ByteBuffer, ByteOrder}

/**
 * Vectorised (batch-at-a-time) page decoders that bypass the per-value
 * `ValuesReader` abstraction used in the scalar path.
 *
 * Each object exposes a single `decode` method that writes directly into the
 * caller-supplied primitive array.
 */
object VectorisedDecoders {

  // ---------------------------------------------------------------------------
  // PLAIN encoding
  // ---------------------------------------------------------------------------

  /** PLAIN INT64 → Long array.  Bulk memcpy via ByteBuffer. */
  def decodePlainLongs(
      data: InputStream,
      target: Array[Long],
      offset: Int,
      count: Int
  ): Int = {
    val byteCount = count * 8
    val bytes = new Array[Byte](byteCount)
    new DataInputStream(data).readFully(bytes)
    val buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
    buf.asLongBuffer().get(target, offset, count)
    offset + count
  }

  /** PLAIN INT32 → Long array.  Bulk-read ints then widen. */
  def decodePlainIntsToLongs(
      data: InputStream,
      target: Array[Long],
      offset: Int,
      count: Int
  ): Int = {
    val byteCount = count * 4
    val bytes = new Array[Byte](byteCount)
    new DataInputStream(data).readFully(bytes)
    val buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
    val ib = buf.asIntBuffer()
    var i = 0
    while (i < count) {
      target(offset + i) = ib.get(i).toLong
      i += 1
    }
    offset + count
  }

  /** PLAIN DOUBLE → Double array.  Bulk memcpy. */
  def decodePlainDoubles(
      data: InputStream,
      target: Array[Double],
      offset: Int,
      count: Int
  ): Int = {
    val byteCount = count * 8
    val bytes = new Array[Byte](byteCount)
    new DataInputStream(data).readFully(bytes)
    val buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
    buf.asDoubleBuffer().get(target, offset, count)
    offset + count
  }

  /** PLAIN FLOAT → Double array.  Bulk-read floats then widen. */
  def decodePlainFloatsToDoubles(
      data: InputStream,
      target: Array[Double],
      offset: Int,
      count: Int
  ): Int = {
    val byteCount = count * 4
    val bytes = new Array[Byte](byteCount)
    new DataInputStream(data).readFully(bytes)
    val buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
    val fb = buf.asFloatBuffer()
    var i = 0
    while (i < count) {
      target(offset + i) = fb.get(i).toDouble
      i += 1
    }
    offset + count
  }

  // ---------------------------------------------------------------------------
  // DELTA_BINARY_PACKED encoding
  // ---------------------------------------------------------------------------

  /**
   * Vectorised DELTA_BINARY_PACKED → Long array.
   *
   * Format (little-endian ULEB128 encoded header):
   *   blockSizeInValues   — values per block (multiple of 128)
   *   miniBlocksPerBlock  — miniblocks in each block
   *   totalValueCount     — number of values encoded
   *   firstValue          — zigzag-encoded first value
   *
   * Then for each block:
   *   minDelta            — zigzag-encoded
   *   bitWidths           — one byte per miniblock
   *   miniblock data      — bit-packed using that width
   *
   * We decode everything into `target` using a prefix-sum over deltas.
   */
  def decodeDeltaBinaryPackedLongs(
      data: InputStream,
      target: Array[Long],
      offset: Int,
      count: Int
  ): Int = {
    val dis = new DataInputStream(data)
    val blockSize = readUnsignedVarLong(dis).toInt
    val miniBlocksPerBlock = readUnsignedVarLong(dis).toInt
    val totalValueCount = readUnsignedVarLong(dis).toInt
    val firstValue = readZigZagLong(dis)

    val valuesPerMiniBlock = blockSize / miniBlocksPerBlock

    var writeIndex = offset
    target(writeIndex) = firstValue
    writeIndex += 1
    var valuesRead = 1
    var lastValue = firstValue

    while (valuesRead < totalValueCount) {
      val minDelta = readZigZagLong(dis)
      val bitWidths = new Array[Int](miniBlocksPerBlock)
      var mb = 0
      while (mb < miniBlocksPerBlock) {
        bitWidths(mb) = dis.readUnsignedByte()
        mb += 1
      }

      mb = 0
      while (mb < miniBlocksPerBlock && valuesRead < totalValueCount) {
        val bw = bitWidths(mb)
        val valuesToRead = math.min(valuesPerMiniBlock, totalValueCount - valuesRead)
        if (bw == 0) {
          // All deltas in this miniblock are exactly minDelta
          var j = 0
          while (j < valuesToRead) {
            lastValue += minDelta
            if (writeIndex < offset + count) {
              target(writeIndex) = lastValue
              writeIndex += 1
            }
            j += 1
            valuesRead += 1
          }
          // Still need to skip remaining slots (padding)
          // Delta binary packed pads miniblocks to valuesPerMiniBlock but bw=0 means 0 bytes
        } else {
          // Read packed bytes for this miniblock
          val totalBits = valuesPerMiniBlock.toLong * bw
          val packedBytes = ((totalBits + 7) / 8).toInt
          val packed = new Array[Byte](packedBytes)
          dis.readFully(packed)

          // Unpack each value
          var bitOffset = 0
          var j = 0
          while (j < valuesPerMiniBlock) {
            if (valuesRead < totalValueCount && j < valuesToRead) {
              val delta = readBitPackedValue(packed, bitOffset, bw)
              lastValue += minDelta + delta
              if (writeIndex < offset + count) {
                target(writeIndex) = lastValue
                writeIndex += 1
              }
              valuesRead += 1
            } else if (valuesRead >= totalValueCount) {
              // padding values — skip
            } else {
              // Read but discard padding
              val delta = readBitPackedValue(packed, bitOffset, bw)
              lastValue += minDelta + delta
              valuesRead += 1
            }
            bitOffset += bw
            j += 1
          }
        }
        mb += 1
      }
    }

    writeIndex
  }

  // ---------------------------------------------------------------------------
  // BYTE_STREAM_SPLIT encoding
  // ---------------------------------------------------------------------------

  /** BYTE_STREAM_SPLIT for 8-byte doubles: deinterleave N streams of 1 byte each. */
  def decodeByteStreamSplitDoubles(
      data: InputStream,
      target: Array[Double],
      offset: Int,
      count: Int
  ): Int = {
    val stride = 8
    val totalBytes = count * stride
    val bytes = new Array[Byte](totalBytes)
    new DataInputStream(data).readFully(bytes)

    // bytes layout: all byte-0s of all values, then all byte-1s, ..., then all byte-7s
    var i = 0
    while (i < count) {
      var assembled = 0L
      var b = 0
      while (b < stride) {
        assembled |= (bytes(b * count + i).toLong & 0xFFL) << (b * 8)
        b += 1
      }
      target(offset + i) = java.lang.Double.longBitsToDouble(assembled)
      i += 1
    }
    offset + count
  }

  /** BYTE_STREAM_SPLIT for 4-byte floats → Double array. */
  def decodeByteStreamSplitFloatsToDoubles(
      data: InputStream,
      target: Array[Double],
      offset: Int,
      count: Int
  ): Int = {
    val stride = 4
    val totalBytes = count * stride
    val bytes = new Array[Byte](totalBytes)
    new DataInputStream(data).readFully(bytes)

    var i = 0
    while (i < count) {
      var assembled = 0
      var b = 0
      while (b < stride) {
        assembled |= (bytes(b * count + i).toInt & 0xFF) << (b * 8)
        b += 1
      }
      target(offset + i) = java.lang.Float.intBitsToFloat(assembled).toDouble
      i += 1
    }
    offset + count
  }

  /** BYTE_STREAM_SPLIT for 8-byte longs. */
  def decodeByteStreamSplitLongs(
      data: InputStream,
      target: Array[Long],
      offset: Int,
      count: Int
  ): Int = {
    val stride = 8
    val totalBytes = count * stride
    val bytes = new Array[Byte](totalBytes)
    new DataInputStream(data).readFully(bytes)

    var i = 0
    while (i < count) {
      var assembled = 0L
      var b = 0
      while (b < stride) {
        assembled |= (bytes(b * count + i).toLong & 0xFFL) << (b * 8)
        b += 1
      }
      target(offset + i) = assembled
      i += 1
    }
    offset + count
  }

  /** BYTE_STREAM_SPLIT for 4-byte ints → Long array. */
  def decodeByteStreamSplitIntsToLongs(
      data: InputStream,
      target: Array[Long],
      offset: Int,
      count: Int
  ): Int = {
    val stride = 4
    val totalBytes = count * stride
    val bytes = new Array[Byte](totalBytes)
    new DataInputStream(data).readFully(bytes)

    var i = 0
    while (i < count) {
      var assembled = 0
      var b = 0
      while (b < stride) {
        assembled |= (bytes(b * count + i).toInt & 0xFF) << (b * 8)
        b += 1
      }
      target(offset + i) = assembled.toLong
      i += 1
    }
    offset + count
  }

  // ---------------------------------------------------------------------------
  // helpers
  // ---------------------------------------------------------------------------

  private def readUnsignedVarLong(dis: DataInputStream): Long = {
    var result = 0L
    var shift = 0
    var b = dis.readUnsignedByte()
    while ((b & 0x80) != 0) {
      result |= (b.toLong & 0x7F) << shift
      shift += 7
      b = dis.readUnsignedByte()
    }
    result | (b.toLong << shift)
  }

  private def readZigZagLong(dis: DataInputStream): Long = {
    val n = readUnsignedVarLong(dis)
    (n >>> 1) ^ -(n & 1)
  }

  /** Read a `bitWidth`-bit unsigned value starting at `bitOffset` bits into `packed`. */
  private def readBitPackedValue(packed: Array[Byte], bitOffset: Int, bitWidth: Int): Long = {
    if (bitWidth == 0) return 0L
    var value = 0L
    var bitsLeft = bitWidth
    var currentBitOffset = bitOffset

    while (bitsLeft > 0) {
      val byteIndex = currentBitOffset / 8
      val bitIndex = currentBitOffset % 8
      val bitsAvailable = 8 - bitIndex
      val bitsToRead = math.min(bitsLeft, bitsAvailable)
      val mask = (1 << bitsToRead) - 1
      val bits = (packed(byteIndex) >>> bitIndex) & mask
      value |= (bits.toLong & 0xFFL) << (bitWidth - bitsLeft)
      bitsLeft -= bitsToRead
      currentBitOffset += bitsToRead
    }

    value
  }
}

