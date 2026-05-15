package com.example.segmentagg.parquet

import java.math.{BigDecimal => JBigDecimal}

import scala.annotation.nowarn

import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.page.{DataPage, DataPageV1, DataPageV2, PageReadStore}
import org.apache.parquet.column.values.ValuesReader
import org.apache.parquet.column.values.dictionary.DictionaryValuesReader
import org.apache.parquet.column.{Dictionary, ValuesType}
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData
import org.apache.parquet.schema.{LogicalTypeAnnotation, PrimitiveType}

import scala.jdk.CollectionConverters._

// ---------------------------------------------------------------------------
// Trait — the single contract every column chunk decoder must honour
// ---------------------------------------------------------------------------

trait ColumnChunkDecoder[A] {
  def columnName: String
  def descriptor: ColumnDescriptor
  def supports(columnMeta: ColumnChunkMetaData): Boolean
  def decode(pageStore: PageReadStore, target: Array[A], rowCount: Int): Unit
}

// ---------------------------------------------------------------------------
// Scalar base class — page-by-page iteration via Parquet ValuesReader
// ---------------------------------------------------------------------------

abstract class PageLevelColumnChunkDecoder[A](
    val columnName: String,
    val descriptor: ColumnDescriptor
) extends ColumnChunkDecoder[A] with ParquetPageHelpers {

  override def decode(pageStore: PageReadStore, target: Array[A], rowCount: Int): Unit = {
    val pageReader = Option(pageStore.getPageReader(descriptor))
      .getOrElse(throw new IllegalArgumentException(s"Missing page reader for $columnName"))

    val dictionary = readDictionary(pageReader)
    var offset = 0
    var page = pageReader.readPage()

    while (page != null) {
      val t0 = System.nanoTime()
      offset = decodePage(page, dictionary, target, offset)
      DecoderPageMetrics.recordPage(columnName, System.nanoTime() - t0)
      page = pageReader.readPage()
    }

    if (offset != rowCount)
      throw new IllegalArgumentException(s"Decoded $offset values for $columnName but expected $rowCount")
  }

  /** Decode a single value from the current reader position. */
  protected def decodeValue(reader: ValuesReader): A

  // -- page dispatch --

  private def decodePage(page: DataPage, dictionary: Option[Dictionary], target: Array[A], offset: Int): Int =
    page.accept(new DataPage.Visitor[Int] {
      override def visit(v1: DataPageV1): Int = decodePageV1(v1, dictionary, target, offset)
      override def visit(v2: DataPageV2): Int = decodePageV2(v2, dictionary, target, offset)
    })

  protected def decodePageV1(
      page: DataPageV1,
      dictionary: Option[Dictionary],
      target: Array[A],
      offset: Int
  ): Int = {
    val input      = page.getBytes.toInputStream
    val valueCount = page.getValueCount
    val rl = valuesReader(page.getRlEncoding, ValuesType.REPETITION_LEVEL, dictionary)
    rl.initFromPage(valueCount, input)
    val dl = valuesReader(page.getDlEncoding, ValuesType.DEFINITION_LEVEL, dictionary)
    dl.initFromPage(valueCount, input)
    val values = valuesReader(page.getValueEncoding, ValuesType.VALUES, dictionary)
    values.initFromPage(valueCount, input)

    scalarDecodeValues(
      valueCount,
      new ValuesReaderIntIterator(rl),
      new ValuesReaderIntIterator(dl),
      values, target, offset
    )
  }

  protected def decodePageV2(
      page: DataPageV2,
      dictionary: Option[Dictionary],
      target: Array[A],
      offset: Int
  ): Int = {
    val rl     = rleIterator(descriptor.getMaxRepetitionLevel, page.getRepetitionLevels)
    val dl     = rleIterator(descriptor.getMaxDefinitionLevel, page.getDefinitionLevels)
    val values = valuesReader(page.getDataEncoding, ValuesType.VALUES, dictionary)
    values.initFromPage(page.getValueCount, page.getData.toInputStream)
    scalarDecodeValues(page.getValueCount, rl, dl, values, target, offset)
  }

  /** Row-at-a-time decode loop — the generic scalar fallback. */
  protected final def scalarDecodeValues(
      valueCount: Int,
      rl: IntIterator,
      dl: IntIterator,
      values: ValuesReader,
      target: Array[A],
      offset: Int
  ): Int = {
    var idx = offset
    var i   = 0
    while (i < valueCount) {
      requireNonRepeatedNonNull(rl.nextInt(), dl.nextInt())
      target(idx) = decodeValue(values)
      idx += 1
      i   += 1
    }
    idx
  }
}

// ---------------------------------------------------------------------------
// Scalar Long decoder — supports PLAIN, DELTA_BINARY_PACKED, etc.
// ---------------------------------------------------------------------------

final class LongColumnChunkDecoder(
    override val columnName: String,
    override val descriptor: ColumnDescriptor
) extends PageLevelColumnChunkDecoder[Long](columnName, descriptor) {

  private val primitiveType = descriptor.getPrimitiveType.getPrimitiveTypeName

  override def supports(columnMeta: ColumnChunkMetaData): Boolean =
    columnMeta.getPrimitiveType.getPrimitiveTypeName == descriptor.getPrimitiveType.getPrimitiveTypeName

  override protected def decodeValue(reader: ValuesReader): Long =
    primitiveType match {
      case PrimitiveType.PrimitiveTypeName.INT64 => reader.readLong()
      case PrimitiveType.PrimitiveTypeName.INT32 => reader.readInteger().toLong
      case other => throw new IllegalArgumentException(s"Unsupported integer type for $columnName: $other")
    }
}

// ---------------------------------------------------------------------------
// Scalar dictionary-optimised Long decoder
// ---------------------------------------------------------------------------

final class DictionaryLongColumnChunkDecoder(
    override val columnName: String,
    override val descriptor: ColumnDescriptor
) extends PageLevelColumnChunkDecoder[Long](columnName, descriptor) {

  import java.io.DataInputStream
  import org.apache.parquet.bytes.BytesUtils
  import org.apache.parquet.column.values.bitpacking.Packer

  private val primitiveType = descriptor.getPrimitiveType.getPrimitiveTypeName

  override def supports(columnMeta: ColumnChunkMetaData): Boolean =
    columnMeta.getPrimitiveType.getPrimitiveTypeName == descriptor.getPrimitiveType.getPrimitiveTypeName &&
      columnMeta.getEncodings.asScala.exists(_.usesDictionary)

  override protected def decodeValue(reader: ValuesReader): Long =
    primitiveType match {
      case PrimitiveType.PrimitiveTypeName.INT64 => reader.readLong()
      case PrimitiveType.PrimitiveTypeName.INT32 => reader.readInteger().toLong
      case other => throw new IllegalArgumentException(s"Unsupported integer type for $columnName: $other")
    }

  override protected def decodePageV1(
      page: DataPageV1,
      dictionary: Option[Dictionary],
      target: Array[Long],
      offset: Int
  ): Int = {
    val input      = page.getBytes.toInputStream
    val valueCount = page.getValueCount
    val rl = valuesReader(page.getRlEncoding, ValuesType.REPETITION_LEVEL, dictionary)
    rl.initFromPage(valueCount, input)
    val dl = valuesReader(page.getDlEncoding, ValuesType.DEFINITION_LEVEL, dictionary)
    dl.initFromPage(valueCount, input)

    val pageNullCount = Option(page.getStatistics).filter(_.isNumNullsSet).map(_.getNumNulls).getOrElse(-1L)
    if (dictionary.isDefined && canBulkDecode(pageNullCount))
      return bulkDecodeDictionaryPage(valueCount, dictionary.get, input, target, offset)

    val values = valuesReader(page.getValueEncoding, ValuesType.VALUES, dictionary)
    values.initFromPage(valueCount, input)
    decodeDictionaryValues(
      valueCount,
      new ValuesReaderIntIterator(rl),
      new ValuesReaderIntIterator(dl),
      dictionary, values, target, offset
    )
  }

  override protected def decodePageV2(
      page: DataPageV2,
      dictionary: Option[Dictionary],
      target: Array[Long],
      offset: Int
  ): Int = {
    if (dictionary.isDefined && canBulkDecode(page.getNullCount.toLong))
      return bulkDecodeDictionaryPage(page.getValueCount, dictionary.get, page.getData.toInputStream, target, offset)

    val rl     = rleIterator(descriptor.getMaxRepetitionLevel, page.getRepetitionLevels)
    val dl     = rleIterator(descriptor.getMaxDefinitionLevel, page.getDefinitionLevels)
    val values = valuesReader(page.getDataEncoding, ValuesType.VALUES, dictionary)
    values.initFromPage(page.getValueCount, page.getData.toInputStream)
    decodeDictionaryValues(page.getValueCount, rl, dl, dictionary, values, target, offset)
  }

  // -- dictionary-aware scalar loop --

  private def decodeDictionaryValues(
      valueCount: Int,
      rl: IntIterator,
      dl: IntIterator,
      dictionary: Option[Dictionary],
      values: ValuesReader,
      target: Array[Long],
      offset: Int
  ): Int = {
    (for {
      dictReader <- values match {
        case r: DictionaryValuesReader => Some(r)
        case _ => None
      }
      dict <- dictionary
    } yield {
      val dictValues = materializeDictionary(dict)
      var idx = offset
      var i   = 0
      while (i < valueCount) {
        requireNonRepeatedNonNull(rl.nextInt(), dl.nextInt())
        target(idx) = dictValues(dictReader.readValueDictionaryId())
        idx += 1
        i   += 1
      }
      idx
    }).getOrElse(scalarDecodeValues(valueCount, rl, dl, values, target, offset))
  }

  // -- bulk dictionary decode (skips rep/def) --

  private def canBulkDecode(pageNullCount: Long): Boolean =
    descriptor.getMaxRepetitionLevel == 0 && pageNullCount == 0

  @nowarn("msg=deprecated")
  private def bulkDecodeDictionaryPage(
      valueCount: Int,
      dictionary: Dictionary,
      data: java.io.InputStream,
      target: Array[Long],
      offset: Int
  ): Int = {
    val dictValues = materializeDictionary(dictionary)
    val bitWidth   = BytesUtils.readIntLittleEndianOnOneByte(data)
    if (bitWidth == 0) {
      java.util.Arrays.fill(target, offset, offset + valueCount, dictValues(0))
      return offset + valueCount
    }

    val packer    = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth)
    val packedIds = new Array[Int](8)
    var targetIdx = offset
    var remaining = valueCount

    while (remaining > 0) {
      val header = BytesUtils.readUnsignedVarInt(data)
      if ((header & 1) == 0) {
        val runLength    = header >>> 1
        val decodedValue = dictValues(BytesUtils.readIntLittleEndianPaddedOnBitWidth(data, bitWidth))
        java.util.Arrays.fill(target, targetIdx, targetIdx + runLength, decodedValue)
        targetIdx += runLength
        remaining -= runLength
      } else {
        val groupCount     = header >>> 1
        val packedByteCount = groupCount * bitWidth
        val packedBytes    = new Array[Byte](packedByteCount)
        new DataInputStream(data).readFully(packedBytes)

        var gi = 0; var bi = 0
        while (gi < groupCount && remaining > 0) {
          packer.unpack8Values(packedBytes, bi, packedIds, 0)
          var id = 0
          while (id < 8 && remaining > 0) {
            target(targetIdx) = dictValues(packedIds(id))
            targetIdx += 1; remaining -= 1; id += 1
          }
          gi += 1; bi += bitWidth
        }
      }
    }
    targetIdx
  }

  private def materializeDictionary(dictionary: Dictionary): Array[Long] = {
    val max    = dictionary.getMaxId
    val values = new Array[Long](max + 1)
    var i = 0
    while (i <= max) {
      values(i) = primitiveType match {
        case PrimitiveType.PrimitiveTypeName.INT64 => dictionary.decodeToLong(i)
        case PrimitiveType.PrimitiveTypeName.INT32 => dictionary.decodeToInt(i).toLong
        case other => throw new IllegalArgumentException(s"Unsupported integer type for $columnName: $other")
      }
      i += 1
    }
    values
  }
}

// ---------------------------------------------------------------------------
// Scalar Double decoder — supports PLAIN, DECIMAL, FLOAT, etc.
// ---------------------------------------------------------------------------

final class DoubleColumnChunkDecoder(
    override val columnName: String,
    override val descriptor: ColumnDescriptor
) extends PageLevelColumnChunkDecoder[Double](columnName, descriptor) {

  private val primitiveType = descriptor.getPrimitiveType.getPrimitiveTypeName
  private val logicalType   = descriptor.getPrimitiveType.getLogicalTypeAnnotation

  override def supports(columnMeta: ColumnChunkMetaData): Boolean =
    columnMeta.getPrimitiveType == descriptor.getPrimitiveType

  override protected def decodeValue(reader: ValuesReader): Double =
    (logicalType, primitiveType) match {
      case (d: LogicalTypeAnnotation.DecimalLogicalTypeAnnotation, PrimitiveType.PrimitiveTypeName.BINARY) =>
        DecimalConversions.decimalFromBinary(reader.readBytes(), d.getScale)
      case (d: LogicalTypeAnnotation.DecimalLogicalTypeAnnotation, PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) =>
        DecimalConversions.fixedLengthDecimalToDouble(reader.readBytes(), d.getScale)
      case (d: LogicalTypeAnnotation.DecimalLogicalTypeAnnotation, PrimitiveType.PrimitiveTypeName.INT32) =>
        JBigDecimal.valueOf(reader.readInteger().toLong, d.getScale).doubleValue()
      case (d: LogicalTypeAnnotation.DecimalLogicalTypeAnnotation, PrimitiveType.PrimitiveTypeName.INT64) =>
        JBigDecimal.valueOf(reader.readLong(), d.getScale).doubleValue()
      case (_, PrimitiveType.PrimitiveTypeName.DOUBLE) =>
        reader.readDouble()
      case (_, PrimitiveType.PrimitiveTypeName.FLOAT) =>
        reader.readFloat().toDouble
      case other =>
        throw new IllegalArgumentException(s"Unsupported revenue type for $columnName: $other")
    }
}

