package com.example.segmentagg.parquet

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayInputStream
import java.nio.{ByteBuffer, ByteOrder}

/**
 * Tests for [[VectorisedDecoders]] — bulk decode routines.
 */
class VectorisedDecodersSpec extends AnyFunSuite with Matchers {

  // -------------------------------------------------------------------------
  // PLAIN Long decoding
  // -------------------------------------------------------------------------

  test("decodePlainLongs reads little-endian INT64 values") {
    val values = Array(1L, 2L, Long.MaxValue, -1L, 0L)
    val bytes = longArrayToLittleEndianBytes(values)
    val target = new Array[Long](values.length)

    val end = VectorisedDecoders.decodePlainLongs(
      new ByteArrayInputStream(bytes), target, 0, values.length
    )

    end shouldBe values.length
    target shouldBe values
  }

  test("decodePlainLongs with offset writes at correct position") {
    val values = Array(42L, 99L)
    val bytes = longArrayToLittleEndianBytes(values)
    val target = new Array[Long](5) // extra space

    val end = VectorisedDecoders.decodePlainLongs(
      new ByteArrayInputStream(bytes), target, 2, 2
    )

    end shouldBe 4
    target(2) shouldBe 42L
    target(3) shouldBe 99L
    target(0) shouldBe 0L // untouched
  }

  test("decodePlainIntsToLongs widens INT32 to Long") {
    val intValues = Array(1, -1, Int.MaxValue, 0)
    val bytes = intArrayToLittleEndianBytes(intValues)
    val target = new Array[Long](intValues.length)

    val end = VectorisedDecoders.decodePlainIntsToLongs(
      new ByteArrayInputStream(bytes), target, 0, intValues.length
    )

    end shouldBe intValues.length
    target shouldBe intValues.map(_.toLong)
  }

  // -------------------------------------------------------------------------
  // PLAIN Double decoding
  // -------------------------------------------------------------------------

  test("decodePlainDoubles reads little-endian DOUBLE values") {
    val values = Array(1.0, -0.5, Double.MaxValue, 0.0, Math.PI)
    val bytes = doubleArrayToLittleEndianBytes(values)
    val target = new Array[Double](values.length)

    val end = VectorisedDecoders.decodePlainDoubles(
      new ByteArrayInputStream(bytes), target, 0, values.length
    )

    end shouldBe values.length
    target shouldBe values
  }

  test("decodePlainFloatsToDoubles widens FLOAT to Double") {
    val floatValues = Array(1.0f, -0.5f, 0.0f, 3.14f)
    val bytes = floatArrayToLittleEndianBytes(floatValues)
    val target = new Array[Double](floatValues.length)

    val end = VectorisedDecoders.decodePlainFloatsToDoubles(
      new ByteArrayInputStream(bytes), target, 0, floatValues.length
    )

    end shouldBe floatValues.length
    target.zip(floatValues).foreach { case (actual, expected) =>
      actual shouldBe (expected.toDouble +- 1e-6)
    }
  }

  // -------------------------------------------------------------------------
  // BYTE_STREAM_SPLIT decoding
  // -------------------------------------------------------------------------

  test("decodeByteStreamSplitDoubles deinterleaves correctly") {
    // Encode 3 doubles in byte-stream-split format
    val values = Array(1.0, 2.0, 3.0)
    val count = values.length
    val stride = 8
    val encoded = new Array[Byte](count * stride)

    // Scatter: byte b of value i goes to position b*count + i
    values.zipWithIndex.foreach { case (v, i) =>
      val bits = java.lang.Double.doubleToLongBits(v)
      var b = 0
      while (b < stride) {
        encoded(b * count + i) = ((bits >>> (b * 8)) & 0xFF).toByte
        b += 1
      }
    }

    val target = new Array[Double](count)
    val end = VectorisedDecoders.decodeByteStreamSplitDoubles(
      new ByteArrayInputStream(encoded), target, 0, count
    )

    end shouldBe count
    target shouldBe values
  }

  test("decodeByteStreamSplitLongs deinterleaves correctly") {
    val values = Array(100L, 200L, -1L)
    val count = values.length
    val stride = 8
    val encoded = new Array[Byte](count * stride)

    values.zipWithIndex.foreach { case (v, i) =>
      var b = 0
      while (b < stride) {
        encoded(b * count + i) = ((v >>> (b * 8)) & 0xFF).toByte
        b += 1
      }
    }

    val target = new Array[Long](count)
    val end = VectorisedDecoders.decodeByteStreamSplitLongs(
      new ByteArrayInputStream(encoded), target, 0, count
    )

    end shouldBe count
    target shouldBe values
  }

  // -------------------------------------------------------------------------
  // Helpers: convert primitive arrays to little-endian byte arrays
  // -------------------------------------------------------------------------

  private def longArrayToLittleEndianBytes(arr: Array[Long]): Array[Byte] = {
    val buf = ByteBuffer.allocate(arr.length * 8).order(ByteOrder.LITTLE_ENDIAN)
    arr.foreach(buf.putLong)
    buf.array()
  }

  private def intArrayToLittleEndianBytes(arr: Array[Int]): Array[Byte] = {
    val buf = ByteBuffer.allocate(arr.length * 4).order(ByteOrder.LITTLE_ENDIAN)
    arr.foreach(buf.putInt)
    buf.array()
  }

  private def doubleArrayToLittleEndianBytes(arr: Array[Double]): Array[Byte] = {
    val buf = ByteBuffer.allocate(arr.length * 8).order(ByteOrder.LITTLE_ENDIAN)
    arr.foreach(buf.putDouble)
    buf.array()
  }

  private def floatArrayToLittleEndianBytes(arr: Array[Float]): Array[Byte] = {
    val buf = ByteBuffer.allocate(arr.length * 4).order(ByteOrder.LITTLE_ENDIAN)
    arr.foreach(buf.putFloat)
    buf.array()
  }
}

