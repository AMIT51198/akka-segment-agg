package com.example.segmentagg.parquet

import java.math.{BigDecimal => JBigDecimal, BigInteger}

import org.apache.parquet.io.api.Binary

/** Reusable Parquet decimal → Double conversion routines. */
private[parquet] object DecimalConversions {

  def decimalFromBinary(binary: Binary, scale: Int): Double =
    new JBigDecimal(new BigInteger(binary.getBytesUnsafe), scale).doubleValue()

  /**
   * Fast path for 16-byte fixed-length decimal values that fit in a Long.
   * Falls back to [[decimalFromBinary]] when the magnitude overflows 8 bytes.
   */
  def fixedLengthDecimalToDouble(binary: Binary, scale: Int): Double = {
    val bytes = binary.getBytesUnsafe
    if (bytes.length != 16) return decimalFromBinary(binary, scale)

    val negative = (bytes(0) & 0x80) != 0
    val expectedFill = if (negative) 0xff else 0x00

    var i = 0
    while (i < 8) {
      if ((bytes(i) & 0xff) != expectedFill) return decimalFromBinary(binary, scale)
      i += 1
    }

    var unscaled = 0L
    while (i < 16) {
      unscaled = (unscaled << 8) | (bytes(i) & 0xffL)
      i += 1
    }

    if (negative) unscaled = -((~unscaled) + 1L)
    unscaled / math.pow(10.0, scale.toDouble)
  }
}

