package com.example.segmentagg.parquet

import java.math.{BigDecimal => JBigDecimal, BigInteger}

import org.apache.parquet.bytes.{ByteBufferInputStream, BytesInput, BytesUtils}
import org.apache.parquet.column.page.{DataPage, DataPageV1, DataPageV2, PageReadStore}
import org.apache.parquet.column.values.ValuesReader
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder
import org.apache.parquet.column.{ColumnDescriptor, Dictionary, Encoding, ValuesType}
import org.apache.parquet.io.ParquetDecodingException
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.{LogicalTypeAnnotation, MessageType, PrimitiveType}

/**
 * Decodes a single Parquet column from a row group into a primitive array.
 * Handles PLAIN, DICTIONARY, DELTA_BINARY_PACKED encodings for Long columns,
 * and PLAIN + DECIMAL for Double columns.
 */
sealed trait ColumnDecoder[A] {
  def decode(pageStore: PageReadStore, target: Array[A], rowCount: Int): Unit
}

object ColumnDecoder {

  def forLong(name: String, schema: MessageType): ColumnDecoder[Long] =
    new LongDecoder(name, schema.getColumnDescription(Array(name)))

  def forDouble(name: String, schema: MessageType): ColumnDecoder[Double] =
    new DoubleDecoder(name, schema.getColumnDescription(Array(name)))

  // ─── Long decoder ──────────────────────────────────────────────────────────

  private final class LongDecoder(name: String, desc: ColumnDescriptor) extends ColumnDecoder[Long] {
    private val pType = desc.getPrimitiveType.getPrimitiveTypeName

    override def decode(pageStore: PageReadStore, target: Array[Long], rowCount: Int): Unit = {
      val pageReader = pageStore.getPageReader(desc)
      val dictionary = Option(pageReader.readDictionaryPage()).map(p => p.getEncoding.initDictionary(desc, p))

      var offset = 0
      var page   = pageReader.readPage()
      while (page != null) {
        offset = page.accept(new DataPage.Visitor[Int] {
          override def visit(v1: DataPageV1): Int = decodeV1(v1, dictionary, target, offset)
          override def visit(v2: DataPageV2): Int = decodeV2(v2, dictionary, target, offset)
        })
        page = pageReader.readPage()
      }
      require(offset == rowCount, s"Decoded $offset values for $name but expected $rowCount")
    }

    private def decodeV1(page: DataPageV1, dict: Option[Dictionary], target: Array[Long], offset: Int): Int = {
      val input = page.getBytes.toInputStream
      val n     = page.getValueCount
      val rl    = levelsReader(page.getRlEncoding, ValuesType.REPETITION_LEVEL, dict, n, input)
      val dl    = levelsReader(page.getDlEncoding, ValuesType.DEFINITION_LEVEL, dict, n, input)
      val vals  = valuesReader(page.getValueEncoding, dict, n, input)
      scalarRead(n, rl, dl, vals, target, offset)
    }

    private def decodeV2(page: DataPageV2, dict: Option[Dictionary], target: Array[Long], offset: Int): Int = {
      val rl   = rleIter(desc.getMaxRepetitionLevel, page.getRepetitionLevels)
      val dl   = rleIter(desc.getMaxDefinitionLevel, page.getDefinitionLevels)
      val vals = valuesReader(page.getDataEncoding, dict, page.getValueCount, page.getData.toInputStream)
      scalarRead(page.getValueCount, rl, dl, vals, target, offset)
    }

    private def scalarRead(
        n: Int, rl: IntIter, dl: IntIter, vals: ValuesReader, target: Array[Long], offset: Int
    ): Int = {
      var idx = offset; var i = 0
      while (i < n) {
        checkLevels(rl.next(), dl.next(), name, desc)
        target(idx) = readLong(vals)
        idx += 1; i += 1
      }
      idx
    }

    private def readLong(r: ValuesReader): Long = pType match {
      case PrimitiveType.PrimitiveTypeName.INT64 => r.readLong()
      case PrimitiveType.PrimitiveTypeName.INT32 => r.readInteger().toLong
      case other => throw new IllegalArgumentException(s"Unsupported type for $name: $other")
    }

    private def levelsReader(enc: Encoding, vt: ValuesType, dict: Option[Dictionary], n: Int, in: ByteBufferInputStream): IntIter = {
      val r = if (enc.usesDictionary) enc.getDictionaryBasedValuesReader(desc, vt, dict.orNull)
              else enc.getValuesReader(desc, vt)
      r.initFromPage(n, in)
      new ValuesReaderIter(r)
    }

    private def valuesReader(enc: Encoding, dict: Option[Dictionary], n: Int, in: ByteBufferInputStream): ValuesReader = {
      val r = if (enc.usesDictionary) {
        val d = dict.getOrElse(throw new ParquetDecodingException(s"Missing dictionary for $name"))
        enc.getDictionaryBasedValuesReader(desc, ValuesType.VALUES, d)
      } else enc.getValuesReader(desc, ValuesType.VALUES)
      r.initFromPage(n, in)
      r
    }
  }

  // ─── Double decoder ────────────────────────────────────────────────────────

  private final class DoubleDecoder(name: String, desc: ColumnDescriptor) extends ColumnDecoder[Double] {
    private val pType  = desc.getPrimitiveType.getPrimitiveTypeName
    private val lType  = desc.getPrimitiveType.getLogicalTypeAnnotation

    override def decode(pageStore: PageReadStore, target: Array[Double], rowCount: Int): Unit = {
      val pageReader = pageStore.getPageReader(desc)
      val dictionary = Option(pageReader.readDictionaryPage()).map(p => p.getEncoding.initDictionary(desc, p))

      var offset = 0
      var page   = pageReader.readPage()
      while (page != null) {
        offset = page.accept(new DataPage.Visitor[Int] {
          override def visit(v1: DataPageV1): Int = decodeV1(v1, dictionary, target, offset)
          override def visit(v2: DataPageV2): Int = decodeV2(v2, dictionary, target, offset)
        })
        page = pageReader.readPage()
      }
      require(offset == rowCount, s"Decoded $offset values for $name but expected $rowCount")
    }

    private def decodeV1(page: DataPageV1, dict: Option[Dictionary], target: Array[Double], offset: Int): Int = {
      val input = page.getBytes.toInputStream
      val n     = page.getValueCount
      val rl    = levelsReader(page.getRlEncoding, ValuesType.REPETITION_LEVEL, dict, n, input)
      val dl    = levelsReader(page.getDlEncoding, ValuesType.DEFINITION_LEVEL, dict, n, input)
      val vals  = valuesReader(page.getValueEncoding, dict, n, input)
      scalarRead(n, rl, dl, vals, target, offset)
    }

    private def decodeV2(page: DataPageV2, dict: Option[Dictionary], target: Array[Double], offset: Int): Int = {
      val rl   = rleIter(desc.getMaxRepetitionLevel, page.getRepetitionLevels)
      val dl   = rleIter(desc.getMaxDefinitionLevel, page.getDefinitionLevels)
      val vals = valuesReader(page.getDataEncoding, dict, page.getValueCount, page.getData.toInputStream)
      scalarRead(page.getValueCount, rl, dl, vals, target, offset)
    }

    private def scalarRead(
        n: Int, rl: IntIter, dl: IntIter, vals: ValuesReader, target: Array[Double], offset: Int
    ): Int = {
      var idx = offset; var i = 0
      while (i < n) {
        checkLevels(rl.next(), dl.next(), name, desc)
        target(idx) = readDouble(vals)
        idx += 1; i += 1
      }
      idx
    }

    private def readDouble(r: ValuesReader): Double = (lType, pType) match {
      case (d: LogicalTypeAnnotation.DecimalLogicalTypeAnnotation, PrimitiveType.PrimitiveTypeName.BINARY) =>
        decimalFromBinary(r.readBytes(), d.getScale)
      case (d: LogicalTypeAnnotation.DecimalLogicalTypeAnnotation, PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) =>
        decimalFromBinary(r.readBytes(), d.getScale)
      case (d: LogicalTypeAnnotation.DecimalLogicalTypeAnnotation, PrimitiveType.PrimitiveTypeName.INT32) =>
        JBigDecimal.valueOf(r.readInteger().toLong, d.getScale).doubleValue()
      case (d: LogicalTypeAnnotation.DecimalLogicalTypeAnnotation, PrimitiveType.PrimitiveTypeName.INT64) =>
        JBigDecimal.valueOf(r.readLong(), d.getScale).doubleValue()
      case (_, PrimitiveType.PrimitiveTypeName.DOUBLE) => r.readDouble()
      case (_, PrimitiveType.PrimitiveTypeName.FLOAT)  => r.readFloat().toDouble
      case other => throw new IllegalArgumentException(s"Unsupported type for $name: $other")
    }

    private def levelsReader(enc: Encoding, vt: ValuesType, dict: Option[Dictionary], n: Int, in: ByteBufferInputStream): IntIter = {
      val r = if (enc.usesDictionary) enc.getDictionaryBasedValuesReader(desc, vt, dict.orNull)
              else enc.getValuesReader(desc, vt)
      r.initFromPage(n, in)
      new ValuesReaderIter(r)
    }

    private def valuesReader(enc: Encoding, dict: Option[Dictionary], n: Int, in: ByteBufferInputStream): ValuesReader = {
      val r = if (enc.usesDictionary) {
        val d = dict.getOrElse(throw new ParquetDecodingException(s"Missing dictionary for $name"))
        enc.getDictionaryBasedValuesReader(desc, ValuesType.VALUES, d)
      } else enc.getValuesReader(desc, ValuesType.VALUES)
      r.initFromPage(n, in)
      r
    }

    private def decimalFromBinary(binary: Binary, scale: Int): Double =
      new JBigDecimal(new BigInteger(binary.getBytesUnsafe), scale).doubleValue()
  }

  // ─── Shared helpers ────────────────────────────────────────────────────────

  private def checkLevels(rl: Int, dl: Int, name: String, desc: ColumnDescriptor): Unit = {
    if (rl != 0) throw new IllegalArgumentException(s"Repeated field not supported: $name")
    if (dl < desc.getMaxDefinitionLevel) throw new IllegalArgumentException(s"Null not supported: $name")
  }

  private def rleIter(maxLevel: Int, bytes: BytesInput): IntIter =
    if (maxLevel == 0) ZeroIter
    else new RleIter(new RunLengthBitPackingHybridDecoder(
      BytesUtils.getWidthFromMaxInt(maxLevel), bytes.toInputStream))

  // ─── Int iterator for rep/def levels ───────────────────────────────────────

  private sealed trait IntIter { def next(): Int }
  private object ZeroIter extends IntIter { def next(): Int = 0 }
  private final class RleIter(d: RunLengthBitPackingHybridDecoder) extends IntIter {
    def next(): Int = try d.readInt() catch { case e: java.io.IOException => throw new ParquetDecodingException(e) }
  }
  private final class ValuesReaderIter(r: ValuesReader) extends IntIter { def next(): Int = r.readInteger() }
}
