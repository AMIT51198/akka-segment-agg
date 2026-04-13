package com.example.segmentagg.parquet

import java.math.{BigDecimal => JBigDecimal, BigInteger}

import scala.jdk.CollectionConverters._

import org.apache.parquet.bytes.{BytesInput, BytesUtils}
import org.apache.parquet.column.page.{DataPage, DataPageV1, DataPageV2, DictionaryPage, PageReadStore, PageReader}
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder
import org.apache.parquet.column.values.ValuesReader
import org.apache.parquet.column.{ColumnDescriptor, Dictionary, Encoding, ValuesType}
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData
import org.apache.parquet.io.ParquetDecodingException
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.{LogicalTypeAnnotation, PrimitiveType}

trait ColumnChunkDecoder[A] {
  def columnName: String
  def descriptor: ColumnDescriptor

  def supports(columnMeta: ColumnChunkMetaData): Boolean

  def decode(pageStore: PageReadStore, target: Array[A], rowCount: Int): Unit
}

abstract class PageLevelColumnChunkDecoder[A](val columnName: String, val descriptor: ColumnDescriptor)
    extends ColumnChunkDecoder[A] {

  override def decode(pageStore: PageReadStore, target: Array[A], rowCount: Int): Unit = {
    val pageReader = Option(pageStore.getPageReader(descriptor))
      .getOrElse(throw new IllegalArgumentException(s"Missing page reader for $columnName"))

    val dictionary = readDictionary(pageReader)
    var offset = 0
    var page = pageReader.readPage()

    while (page != null) {
      offset = decodePage(page, dictionary, target, offset)
      page = pageReader.readPage()
    }

    if (offset != rowCount) {
      throw new IllegalArgumentException(
        s"Decoded $offset values for $columnName but expected $rowCount"
      )
    }
  }

  protected def decodeValue(reader: ValuesReader): A

  private def readDictionary(pageReader: PageReader): Option[Dictionary] =
    Option(pageReader.readDictionaryPage()).map(initDictionary)

  private def initDictionary(page: DictionaryPage): Dictionary =
    page.getEncoding.initDictionary(descriptor, page)

  private def decodePage(page: DataPage, dictionary: Option[Dictionary], target: Array[A], offset: Int): Int =
    page.accept(new DataPage.Visitor[Int] {
      override def visit(dataPageV1: DataPageV1): Int =
        decodePageV1(dataPageV1, dictionary, target, offset)

      override def visit(dataPageV2: DataPageV2): Int =
        decodePageV2(dataPageV2, dictionary, target, offset)
    })

  private def decodePageV1(
      page: DataPageV1,
      dictionary: Option[Dictionary],
      target: Array[A],
      offset: Int
  ): Int = {
    val repetitionLevels = valuesReader(page.getRlEncoding, ValuesType.REPETITION_LEVEL, dictionary)
    val definitionLevels = valuesReader(page.getDlEncoding, ValuesType.DEFINITION_LEVEL, dictionary)
    val input = page.getBytes.toInputStream
    val valueCount = page.getValueCount

    repetitionLevels.initFromPage(valueCount, input)
    definitionLevels.initFromPage(valueCount, input)

    val values = valuesReader(page.getValueEncoding, ValuesType.VALUES, dictionary)
    values.initFromPage(valueCount, input)

    decodeValues(
      valueCount,
      new ValuesReaderIntIterator(repetitionLevels),
      new ValuesReaderIntIterator(definitionLevels),
      values,
      target,
      offset
    )
  }

  private def decodePageV2(
      page: DataPageV2,
      dictionary: Option[Dictionary],
      target: Array[A],
      offset: Int
  ): Int = {
    val repetitionLevels = rleIterator(descriptor.getMaxRepetitionLevel, page.getRepetitionLevels)
    val definitionLevels = rleIterator(descriptor.getMaxDefinitionLevel, page.getDefinitionLevels)
    val values = valuesReader(page.getDataEncoding, ValuesType.VALUES, dictionary)
    values.initFromPage(page.getValueCount, page.getData.toInputStream)

    decodeValues(page.getValueCount, repetitionLevels, definitionLevels, values, target, offset)
  }

  private def decodeValues(
      valueCount: Int,
      repetitionLevels: IntIterator,
      definitionLevels: IntIterator,
      values: ValuesReader,
      target: Array[A],
      offset: Int
  ): Int = {
    val requiredDefinitionLevel = descriptor.getMaxDefinitionLevel
    var targetIndex = offset
    var valueIndex = 0

    while (valueIndex < valueCount) {
      val repetitionLevel = repetitionLevels.nextInt()
      if (repetitionLevel != 0) {
        throw new IllegalArgumentException(s"Unsupported repeated Parquet field: $columnName")
      }

      val definitionLevel = definitionLevels.nextInt()
      if (definitionLevel < requiredDefinitionLevel) {
        throw new IllegalArgumentException(s"Null values are not supported for $columnName")
      }

      target(targetIndex) = decodeValue(values)
      targetIndex += 1
      valueIndex += 1
    }

    targetIndex
  }

  private def valuesReader(
      encoding: Encoding,
      valuesType: ValuesType,
      dictionary: Option[Dictionary]
  ): ValuesReader =
    if (encoding.usesDictionary) {
      val loadedDictionary = dictionary.getOrElse {
        throw new ParquetDecodingException(s"Missing dictionary for $columnName and encoding $encoding")
      }
      encoding.getDictionaryBasedValuesReader(descriptor, valuesType, loadedDictionary)
    } else {
      encoding.getValuesReader(descriptor, valuesType)
    }

  private def rleIterator(maxLevel: Int, bytes: BytesInput): IntIterator =
    if (maxLevel == 0) {
      ZeroIntIterator
    } else {
      new RleIntIterator(
        new RunLengthBitPackingHybridDecoder(BytesUtils.getWidthFromMaxInt(maxLevel), bytes.toInputStream)
      )
    }
}

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
      case other =>
        throw new IllegalArgumentException(s"Unsupported integer type for $columnName: $other")
    }
}

final class DoubleColumnChunkDecoder(
    override val columnName: String,
    override val descriptor: ColumnDescriptor
) extends PageLevelColumnChunkDecoder[Double](columnName, descriptor) {
  private val primitiveType = descriptor.getPrimitiveType.getPrimitiveTypeName
  private val logicalType = descriptor.getPrimitiveType.getLogicalTypeAnnotation

  override def supports(columnMeta: ColumnChunkMetaData): Boolean =
    columnMeta.getPrimitiveType == descriptor.getPrimitiveType

  override protected def decodeValue(reader: ValuesReader): Double =
    (logicalType, primitiveType) match {
      case (decimal: LogicalTypeAnnotation.DecimalLogicalTypeAnnotation, PrimitiveType.PrimitiveTypeName.BINARY) =>
        decimalFromBinary(reader.readBytes(), decimal.getScale)
      case (decimal: LogicalTypeAnnotation.DecimalLogicalTypeAnnotation, PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) =>
        decimalFromBinary(reader.readBytes(), decimal.getScale)
      case (decimal: LogicalTypeAnnotation.DecimalLogicalTypeAnnotation, PrimitiveType.PrimitiveTypeName.INT32) =>
        JBigDecimal.valueOf(reader.readInteger().toLong, decimal.getScale).doubleValue()
      case (decimal: LogicalTypeAnnotation.DecimalLogicalTypeAnnotation, PrimitiveType.PrimitiveTypeName.INT64) =>
        JBigDecimal.valueOf(reader.readLong(), decimal.getScale).doubleValue()
      case (_, PrimitiveType.PrimitiveTypeName.DOUBLE) =>
        reader.readDouble()
      case (_, PrimitiveType.PrimitiveTypeName.FLOAT) =>
        reader.readFloat().toDouble
      case other =>
        throw new IllegalArgumentException(s"Unsupported revenue type for $columnName: $other")
    }

  private def decimalFromBinary(binary: Binary, scale: Int): Double =
    new JBigDecimal(new BigInteger(binary.getBytesUnsafe), scale).doubleValue()
}

private[parquet] sealed trait IntIterator {
  def nextInt(): Int
}

private[parquet] object ZeroIntIterator extends IntIterator {
  override def nextInt(): Int = 0
}

private[parquet] final class RleIntIterator(decoder: RunLengthBitPackingHybridDecoder) extends IntIterator {
  override def nextInt(): Int =
    try {
      decoder.readInt()
    } catch {
      case error: java.io.IOException => throw new ParquetDecodingException(error)
    }
}

private[parquet] final class ValuesReaderIntIterator(reader: ValuesReader) extends IntIterator {
  override def nextInt(): Int = reader.readInteger()
}
