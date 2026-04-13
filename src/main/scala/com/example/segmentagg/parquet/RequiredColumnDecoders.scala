package com.example.segmentagg.parquet

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.parquet.column.{ColumnDescriptor, Encoding}
import org.apache.parquet.hadoop.metadata.{ColumnChunkMetaData, ParquetMetadata}
import org.apache.parquet.schema.{MessageType, PrimitiveType}

final case class RequiredColumnDecoders(
    segmentIds: ColumnChunkDecoder[Long],
    impressions: ColumnChunkDecoder[Long],
    revenues: ColumnChunkDecoder[Double]
)

object RequiredColumnDecoders {
  private val SupportedLevelEncodingNames = Set("RLE", "BIT_PACKED")
  private val SupportedValueEncodingNames =
    Set("PLAIN", "PLAIN_DICTIONARY", "RLE_DICTIONARY", "DELTA_BINARY_PACKED", "BYTE_STREAM_SPLIT")

  def build(footer: ParquetMetadata, schema: MessageType): RequiredColumnDecoders =
    new Builder(footer, schema).build()

  private final class Builder(footer: ParquetMetadata, schema: MessageType) {
    private val cache = mutable.Map.empty[String, ColumnChunkDecoder[?]]

    def build(): RequiredColumnDecoders =
      RequiredColumnDecoders(
        longDecoder("segment_id"),
        longDecoder("impressions"),
        doubleDecoder("revenue")
      )

    private def longDecoder(columnName: String): ColumnChunkDecoder[Long] =
      cached(columnName) {
        val descriptor = schema.getColumnDescription(Array(columnName))
        validateMetadata(columnName, descriptor)
        new LongColumnChunkDecoder(columnName, descriptor)
      }

    private def doubleDecoder(columnName: String): ColumnChunkDecoder[Double] =
      cached(columnName) {
        val descriptor = schema.getColumnDescription(Array(columnName))
        validateMetadata(columnName, descriptor)
        new DoubleColumnChunkDecoder(columnName, descriptor)
      }

    private def cached[A](columnName: String)(buildDecoder: => ColumnChunkDecoder[A]): ColumnChunkDecoder[A] =
      cache.getOrElseUpdate(columnName, buildDecoder).asInstanceOf[ColumnChunkDecoder[A]]

    private def validateMetadata(columnName: String, descriptor: ColumnDescriptor): Unit = {
      val metas = columnMetas(columnName)
      if (metas.isEmpty) {
        throw new IllegalArgumentException(s"Missing footer metadata for column $columnName")
      }

      metas.foreach { meta =>
        if (meta.getPrimitiveType != descriptor.getPrimitiveType) {
          throw new IllegalArgumentException(
            s"Inconsistent primitive type for $columnName across row groups: ${meta.getPrimitiveType} vs ${descriptor.getPrimitiveType}"
          )
        }

        val unsupportedEncodings = meta.getEncodings.asScala.filterNot(isSupportedEncoding)
        if (unsupportedEncodings.nonEmpty) {
          throw new IllegalArgumentException(
            s"Unsupported encodings for $columnName: ${unsupportedEncodings.mkString(", ")}"
          )
        }
      }
    }

    private def columnMetas(columnName: String): List[ColumnChunkMetaData] =
      footer.getBlocks.asScala.toList.flatMap(_.getColumns.asScala).filter(_.getPath.toDotString == columnName)

    private def isSupportedEncoding(encoding: Encoding): Boolean =
      SupportedLevelEncodingNames.contains(encoding.name()) || SupportedValueEncodingNames.contains(encoding.name())
  }
}
