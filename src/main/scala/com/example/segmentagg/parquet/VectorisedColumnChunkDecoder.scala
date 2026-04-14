package com.example.segmentagg.parquet

import java.io.DataInputStream
import java.math.{BigDecimal => JBigDecimal}

import scala.annotation.nowarn

import org.apache.parquet.bytes.BytesUtils
import org.apache.parquet.column.page.{DataPageV1, DataPageV2, PageReadStore}
import org.apache.parquet.column.values.ValuesReader
import org.apache.parquet.column.values.bitpacking.Packer
import org.apache.parquet.column.{ColumnDescriptor, Dictionary, Encoding, ValuesType}
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData
import org.apache.parquet.schema.{LogicalTypeAnnotation, PrimitiveType}

// ---------------------------------------------------------------------------
// Vectorised Long decoder
// ---------------------------------------------------------------------------

final class VectorisedLongColumnChunkDecoder(
    val columnName: String,
    val descriptor: ColumnDescriptor
) extends ColumnChunkDecoder[Long] with ParquetPageHelpers {

  private val primitiveType = descriptor.getPrimitiveType.getPrimitiveTypeName

  override def supports(columnMeta: ColumnChunkMetaData): Boolean =
    columnMeta.getPrimitiveType.getPrimitiveTypeName == descriptor.getPrimitiveType.getPrimitiveTypeName

  override def decode(pageStore: PageReadStore, target: Array[Long], rowCount: Int): Unit = {
    val pageReader = Option(pageStore.getPageReader(descriptor))
      .getOrElse(throw new IllegalArgumentException(s"Missing page reader for $columnName"))

    val dictionary = readDictionary(pageReader)
    var offset = 0
    var page = pageReader.readPage()

    while (page != null) {
      val t0 = System.nanoTime()
      offset = page.accept(new org.apache.parquet.column.page.DataPage.Visitor[Int] {
        override def visit(v1: DataPageV1): Int = decodePageV1(v1, dictionary, target, offset)
        override def visit(v2: DataPageV2): Int = decodePageV2(v2, dictionary, target, offset)
      })
      DecoderPageMetrics.recordPage(columnName, System.nanoTime() - t0)
      page = pageReader.readPage()
    }

    if (offset != rowCount)
      throw new IllegalArgumentException(s"Decoded $offset values for $columnName but expected $rowCount")
  }

  // -- V1 pages --

  private def decodePageV1(
      page: DataPageV1, dictionary: Option[Dictionary], target: Array[Long], offset: Int
  ): Int = {
    val valueCount    = page.getValueCount
    val encoding      = page.getValueEncoding
    val input         = page.getBytes.toInputStream
    val pageNullCount = Option(page.getStatistics).filter(_.isNumNullsSet).map(_.getNumNulls).getOrElse(-1L)
    val canBulk       = descriptor.getMaxRepetitionLevel == 0 && pageNullCount == 0

    if (canBulk) {
      // Consume rep/def level bytes
      val rl = valuesReader(page.getRlEncoding, ValuesType.REPETITION_LEVEL, dictionary)
      rl.initFromPage(valueCount, input)
      val dl = valuesReader(page.getDlEncoding, ValuesType.DEFINITION_LEVEL, dictionary)
      dl.initFromPage(valueCount, input)

      encoding match {
        case Encoding.PLAIN              => decodePlainBulk(input, target, offset, valueCount)
        case Encoding.DELTA_BINARY_PACKED => VectorisedDecoders.decodeDeltaBinaryPackedLongs(input, target, offset, valueCount)
        case Encoding.BYTE_STREAM_SPLIT  => decodeBSSBulk(input, target, offset, valueCount)
        case _ if encoding.usesDictionary && dictionary.isDefined =>
          bulkDecodeDictionary(valueCount, dictionary.get, input, target, offset)
        case _ => scalarFallbackV1(page, dictionary, target, offset)
      }
    } else scalarFallbackV1(page, dictionary, target, offset)
  }

  // -- V2 pages --

  private def decodePageV2(
      page: DataPageV2, dictionary: Option[Dictionary], target: Array[Long], offset: Int
  ): Int = {
    val valueCount = page.getValueCount
    val encoding   = page.getDataEncoding
    val canBulk    = descriptor.getMaxRepetitionLevel == 0 && page.getNullCount == 0

    if (canBulk) {
      val data = page.getData.toInputStream
      encoding match {
        case Encoding.PLAIN              => decodePlainBulk(data, target, offset, valueCount)
        case Encoding.DELTA_BINARY_PACKED => VectorisedDecoders.decodeDeltaBinaryPackedLongs(data, target, offset, valueCount)
        case Encoding.BYTE_STREAM_SPLIT  => decodeBSSBulk(data, target, offset, valueCount)
        case _ if encoding.usesDictionary && dictionary.isDefined =>
          bulkDecodeDictionary(valueCount, dictionary.get, page.getData.toInputStream, target, offset)
        case _ => scalarFallbackV2(page, dictionary, target, offset)
      }
    } else scalarFallbackV2(page, dictionary, target, offset)
  }

  // -- vectorised PLAIN --

  private def decodePlainBulk(data: java.io.InputStream, target: Array[Long], offset: Int, count: Int): Int =
    primitiveType match {
      case PrimitiveType.PrimitiveTypeName.INT64 => VectorisedDecoders.decodePlainLongs(data, target, offset, count)
      case PrimitiveType.PrimitiveTypeName.INT32 => VectorisedDecoders.decodePlainIntsToLongs(data, target, offset, count)
      case other => throw new IllegalArgumentException(s"Unsupported integer type for $columnName: $other")
    }

  // -- vectorised BYTE_STREAM_SPLIT --

  private def decodeBSSBulk(data: java.io.InputStream, target: Array[Long], offset: Int, count: Int): Int =
    primitiveType match {
      case PrimitiveType.PrimitiveTypeName.INT64 => VectorisedDecoders.decodeByteStreamSplitLongs(data, target, offset, count)
      case PrimitiveType.PrimitiveTypeName.INT32 => VectorisedDecoders.decodeByteStreamSplitIntsToLongs(data, target, offset, count)
      case other => throw new IllegalArgumentException(s"Unsupported integer type for $columnName: $other")
    }

  // -- vectorised RLE_DICTIONARY --

  @nowarn("msg=deprecated")
  private def bulkDecodeDictionary(
      valueCount: Int, dictionary: Dictionary, data: java.io.InputStream, target: Array[Long], offset: Int
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
        val runLen = header >>> 1
        val value  = dictValues(BytesUtils.readIntLittleEndianPaddedOnBitWidth(data, bitWidth))
        java.util.Arrays.fill(target, targetIdx, targetIdx + runLen, value)
        targetIdx += runLen; remaining -= runLen
      } else {
        val groupCount = header >>> 1
        val packed     = new Array[Byte](groupCount * bitWidth)
        new DataInputStream(data).readFully(packed)

        var gi = 0; var bi = 0
        while (gi < groupCount && remaining > 0) {
          packer.unpack8Values(packed, bi, packedIds, 0)
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

  // -- scalar fallbacks --

  private def scalarFallbackV1(
      page: DataPageV1, dictionary: Option[Dictionary], target: Array[Long], offset: Int
  ): Int = {
    val input      = page.getBytes.toInputStream
    val valueCount = page.getValueCount
    val rl = valuesReader(page.getRlEncoding, ValuesType.REPETITION_LEVEL, dictionary)
    rl.initFromPage(valueCount, input)
    val dl = valuesReader(page.getDlEncoding, ValuesType.DEFINITION_LEVEL, dictionary)
    dl.initFromPage(valueCount, input)
    val values = valuesReader(page.getValueEncoding, ValuesType.VALUES, dictionary)
    values.initFromPage(valueCount, input)
    scalarLoop(valueCount, new ValuesReaderIntIterator(rl), new ValuesReaderIntIterator(dl), values, target, offset)
  }

  private def scalarFallbackV2(
      page: DataPageV2, dictionary: Option[Dictionary], target: Array[Long], offset: Int
  ): Int = {
    val values = valuesReader(page.getDataEncoding, ValuesType.VALUES, dictionary)
    values.initFromPage(page.getValueCount, page.getData.toInputStream)
    scalarLoop(
      page.getValueCount,
      rleIterator(descriptor.getMaxRepetitionLevel, page.getRepetitionLevels),
      rleIterator(descriptor.getMaxDefinitionLevel, page.getDefinitionLevels),
      values, target, offset
    )
  }

  private def scalarLoop(
      count: Int, rl: IntIterator, dl: IntIterator, values: ValuesReader, target: Array[Long], offset: Int
  ): Int = {
    var idx = offset; var i = 0
    while (i < count) {
      requireNonRepeatedNonNull(rl.nextInt(), dl.nextInt())
      target(idx) = readLongValue(values)
      idx += 1; i += 1
    }
    idx
  }

  private def readLongValue(reader: ValuesReader): Long =
    primitiveType match {
      case PrimitiveType.PrimitiveTypeName.INT64 => reader.readLong()
      case PrimitiveType.PrimitiveTypeName.INT32 => reader.readInteger().toLong
      case other => throw new IllegalArgumentException(s"Unsupported integer type for $columnName: $other")
    }

  private def materializeDictionary(dictionary: Dictionary): Array[Long] = {
    val max = dictionary.getMaxId
    val arr = new Array[Long](max + 1)
    var i = 0
    while (i <= max) {
      arr(i) = primitiveType match {
        case PrimitiveType.PrimitiveTypeName.INT64 => dictionary.decodeToLong(i)
        case PrimitiveType.PrimitiveTypeName.INT32 => dictionary.decodeToInt(i).toLong
        case other => throw new IllegalArgumentException(s"Unsupported integer type for $columnName: $other")
      }
      i += 1
    }
    arr
  }
}

// ---------------------------------------------------------------------------
// Vectorised Double decoder
// ---------------------------------------------------------------------------

final class VectorisedDoubleColumnChunkDecoder(
    val columnName: String,
    val descriptor: ColumnDescriptor
) extends ColumnChunkDecoder[Double] with ParquetPageHelpers {

  private val primitiveType = descriptor.getPrimitiveType.getPrimitiveTypeName
  private val logicalType   = descriptor.getPrimitiveType.getLogicalTypeAnnotation

  /** True when the physical type is DOUBLE or FLOAT without a Decimal annotation. */
  private val isSimpleNumeric: Boolean =
    logicalType == null && (primitiveType == PrimitiveType.PrimitiveTypeName.DOUBLE ||
      primitiveType == PrimitiveType.PrimitiveTypeName.FLOAT)

  override def supports(columnMeta: ColumnChunkMetaData): Boolean =
    columnMeta.getPrimitiveType == descriptor.getPrimitiveType

  override def decode(pageStore: PageReadStore, target: Array[Double], rowCount: Int): Unit = {
    val pageReader = Option(pageStore.getPageReader(descriptor))
      .getOrElse(throw new IllegalArgumentException(s"Missing page reader for $columnName"))

    val dictionary = readDictionary(pageReader)
    var offset = 0
    var page = pageReader.readPage()

    while (page != null) {
      val t0 = System.nanoTime()
      offset = page.accept(new org.apache.parquet.column.page.DataPage.Visitor[Int] {
        override def visit(v1: DataPageV1): Int = decodePageV1(v1, dictionary, target, offset)
        override def visit(v2: DataPageV2): Int = decodePageV2(v2, dictionary, target, offset)
      })
      DecoderPageMetrics.recordPage(columnName, System.nanoTime() - t0)
      page = pageReader.readPage()
    }

    if (offset != rowCount)
      throw new IllegalArgumentException(s"Decoded $offset values for $columnName but expected $rowCount")
  }

  // -- V1 pages --

  private def decodePageV1(
      page: DataPageV1, dictionary: Option[Dictionary], target: Array[Double], offset: Int
  ): Int = {
    val valueCount    = page.getValueCount
    val encoding      = page.getValueEncoding
    val input         = page.getBytes.toInputStream
    val pageNullCount = Option(page.getStatistics).filter(_.isNumNullsSet).map(_.getNumNulls).getOrElse(-1L)
    val canBulk       = descriptor.getMaxRepetitionLevel == 0 && pageNullCount == 0

    if (canBulk && isSimpleNumeric) {
      val rl = valuesReader(page.getRlEncoding, ValuesType.REPETITION_LEVEL, dictionary)
      rl.initFromPage(valueCount, input)
      val dl = valuesReader(page.getDlEncoding, ValuesType.DEFINITION_LEVEL, dictionary)
      dl.initFromPage(valueCount, input)

      encoding match {
        case Encoding.PLAIN             => decodePlainBulk(input, target, offset, valueCount)
        case Encoding.BYTE_STREAM_SPLIT => decodeBSSBulk(input, target, offset, valueCount)
        case _ if encoding.usesDictionary && dictionary.isDefined =>
          bulkDecodeDictionary(valueCount, dictionary.get, input, target, offset)
        case _ => scalarFallbackV1(page, dictionary, target, offset)
      }
    } else scalarFallbackV1(page, dictionary, target, offset)
  }

  // -- V2 pages --

  private def decodePageV2(
      page: DataPageV2, dictionary: Option[Dictionary], target: Array[Double], offset: Int
  ): Int = {
    val valueCount = page.getValueCount
    val encoding   = page.getDataEncoding
    val canBulk    = descriptor.getMaxRepetitionLevel == 0 && page.getNullCount == 0

    if (canBulk && isSimpleNumeric) {
      val data = page.getData.toInputStream
      encoding match {
        case Encoding.PLAIN             => decodePlainBulk(data, target, offset, valueCount)
        case Encoding.BYTE_STREAM_SPLIT => decodeBSSBulk(data, target, offset, valueCount)
        case _ if encoding.usesDictionary && dictionary.isDefined =>
          bulkDecodeDictionary(valueCount, dictionary.get, page.getData.toInputStream, target, offset)
        case _ => scalarFallbackV2(page, dictionary, target, offset)
      }
    } else scalarFallbackV2(page, dictionary, target, offset)
  }

  // -- vectorised PLAIN --

  private def decodePlainBulk(data: java.io.InputStream, target: Array[Double], offset: Int, count: Int): Int =
    primitiveType match {
      case PrimitiveType.PrimitiveTypeName.DOUBLE => VectorisedDecoders.decodePlainDoubles(data, target, offset, count)
      case PrimitiveType.PrimitiveTypeName.FLOAT  => VectorisedDecoders.decodePlainFloatsToDoubles(data, target, offset, count)
      case other => throw new IllegalArgumentException(s"Unsupported double type for $columnName: $other")
    }

  // -- vectorised BYTE_STREAM_SPLIT --

  private def decodeBSSBulk(data: java.io.InputStream, target: Array[Double], offset: Int, count: Int): Int =
    primitiveType match {
      case PrimitiveType.PrimitiveTypeName.DOUBLE => VectorisedDecoders.decodeByteStreamSplitDoubles(data, target, offset, count)
      case PrimitiveType.PrimitiveTypeName.FLOAT  => VectorisedDecoders.decodeByteStreamSplitFloatsToDoubles(data, target, offset, count)
      case other => throw new IllegalArgumentException(s"Unsupported double type for $columnName: $other")
    }

  // -- vectorised RLE_DICTIONARY --

  @nowarn("msg=deprecated")
  private def bulkDecodeDictionary(
      valueCount: Int, dictionary: Dictionary, data: java.io.InputStream, target: Array[Double], offset: Int
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
        val runLen = header >>> 1
        val value  = dictValues(BytesUtils.readIntLittleEndianPaddedOnBitWidth(data, bitWidth))
        java.util.Arrays.fill(target, targetIdx, targetIdx + runLen, value)
        targetIdx += runLen; remaining -= runLen
      } else {
        val groupCount = header >>> 1
        val packed     = new Array[Byte](groupCount * bitWidth)
        new DataInputStream(data).readFully(packed)

        var gi = 0; var bi = 0
        while (gi < groupCount && remaining > 0) {
          packer.unpack8Values(packed, bi, packedIds, 0)
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

  private def materializeDictionary(dictionary: Dictionary): Array[Double] = {
    val max = dictionary.getMaxId
    val arr = new Array[Double](max + 1)
    var i = 0
    while (i <= max) {
      arr(i) = primitiveType match {
        case PrimitiveType.PrimitiveTypeName.DOUBLE => dictionary.decodeToDouble(i)
        case PrimitiveType.PrimitiveTypeName.FLOAT  => dictionary.decodeToFloat(i).toDouble
        case other => throw new IllegalArgumentException(s"Unsupported double type for $columnName: $other")
      }
      i += 1
    }
    arr
  }

  // -- scalar fallbacks --

  private def scalarFallbackV1(
      page: DataPageV1, dictionary: Option[Dictionary], target: Array[Double], offset: Int
  ): Int = {
    val input      = page.getBytes.toInputStream
    val valueCount = page.getValueCount
    val rl = valuesReader(page.getRlEncoding, ValuesType.REPETITION_LEVEL, dictionary)
    rl.initFromPage(valueCount, input)
    val dl = valuesReader(page.getDlEncoding, ValuesType.DEFINITION_LEVEL, dictionary)
    dl.initFromPage(valueCount, input)
    val values = valuesReader(page.getValueEncoding, ValuesType.VALUES, dictionary)
    values.initFromPage(valueCount, input)
    scalarLoop(valueCount, new ValuesReaderIntIterator(rl), new ValuesReaderIntIterator(dl), values, target, offset)
  }

  private def scalarFallbackV2(
      page: DataPageV2, dictionary: Option[Dictionary], target: Array[Double], offset: Int
  ): Int = {
    val values = valuesReader(page.getDataEncoding, ValuesType.VALUES, dictionary)
    values.initFromPage(page.getValueCount, page.getData.toInputStream)
    scalarLoop(
      page.getValueCount,
      rleIterator(descriptor.getMaxRepetitionLevel, page.getRepetitionLevels),
      rleIterator(descriptor.getMaxDefinitionLevel, page.getDefinitionLevels),
      values, target, offset
    )
  }

  private def scalarLoop(
      count: Int, rl: IntIterator, dl: IntIterator, values: ValuesReader, target: Array[Double], offset: Int
  ): Int = {
    var idx = offset; var i = 0
    while (i < count) {
      requireNonRepeatedNonNull(rl.nextInt(), dl.nextInt())
      target(idx) = readDoubleValue(values)
      idx += 1; i += 1
    }
    idx
  }

  private def readDoubleValue(reader: ValuesReader): Double =
    (logicalType, primitiveType) match {
      case (d: LogicalTypeAnnotation.DecimalLogicalTypeAnnotation, PrimitiveType.PrimitiveTypeName.BINARY) =>
        DecimalConversions.decimalFromBinary(reader.readBytes(), d.getScale)
      case (d: LogicalTypeAnnotation.DecimalLogicalTypeAnnotation, PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) =>
        DecimalConversions.fixedLengthDecimalToDouble(reader.readBytes(), d.getScale)
      case (d: LogicalTypeAnnotation.DecimalLogicalTypeAnnotation, PrimitiveType.PrimitiveTypeName.INT32) =>
        JBigDecimal.valueOf(reader.readInteger().toLong, d.getScale).doubleValue()
      case (d: LogicalTypeAnnotation.DecimalLogicalTypeAnnotation, PrimitiveType.PrimitiveTypeName.INT64) =>
        JBigDecimal.valueOf(reader.readLong(), d.getScale).doubleValue()
      case (_, PrimitiveType.PrimitiveTypeName.DOUBLE) => reader.readDouble()
      case (_, PrimitiveType.PrimitiveTypeName.FLOAT)  => reader.readFloat().toDouble
      case other => throw new IllegalArgumentException(s"Unsupported revenue type for $columnName: $other")
    }
}

