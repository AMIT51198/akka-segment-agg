package com.example.segmentagg.parquet

import org.apache.parquet.column.values.ValuesReader
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder
import org.apache.parquet.io.ParquetDecodingException

/**
 * Minimal iterator abstraction for reading repetition / definition levels.
 *
 * Sealed trait with package-private visibility ensures that only the three
 * known implementations exist (Scala best practice for controlled extension).
 */
private[parquet] sealed trait IntIterator {
  def nextInt(): Int
}

/** Constant zero iterator — used when the max level is 0 (non-repeated, required). */
private[parquet] object ZeroIntIterator extends IntIterator {
  override def nextInt(): Int = 0
}

/** Delegates to a Parquet RLE/bit-packing hybrid decoder. */
private[parquet] final class RleIntIterator(decoder: RunLengthBitPackingHybridDecoder) extends IntIterator {
  override def nextInt(): Int =
    try decoder.readInt()
    catch { case e: java.io.IOException => throw new ParquetDecodingException(e) }
}

/** Delegates to a Parquet [[ValuesReader]] for definition/repetition levels. */
private[parquet] final class ValuesReaderIntIterator(reader: ValuesReader) extends IntIterator {
  override def nextInt(): Int = reader.readInteger()
}

