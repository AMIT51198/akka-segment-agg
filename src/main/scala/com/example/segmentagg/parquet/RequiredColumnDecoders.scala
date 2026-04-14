package com.example.segmentagg.parquet

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.parquet.column.{ColumnDescriptor, Encoding}
import org.apache.parquet.hadoop.metadata.{ColumnChunkMetaData, ParquetMetadata}
import org.apache.parquet.schema.MessageType

/**
 * Holds the three column-level decoders required by the aggregation pipeline.
 *
 * Constructed via [[RequiredColumnDecoders.build]], which validates footer
 * metadata and selects the appropriate decoder implementation based on
 * the requested [[DecodeMode]].
 */
final case class RequiredColumnDecoders(
    segmentIds: ColumnChunkDecoder[Long],
    impressions: ColumnChunkDecoder[Long],
    revenues: ColumnChunkDecoder[Double]
)

object RequiredColumnDecoders {

  private val SupportedLevelEncodings = Set("RLE", "BIT_PACKED")
  private val SupportedValueEncodings =
    Set("PLAIN", "PLAIN_DICTIONARY", "RLE_DICTIONARY", "DELTA_BINARY_PACKED", "BYTE_STREAM_SPLIT")

  def build(
      footer: ParquetMetadata,
      schema: MessageType,
      decodeMode: DecodeMode = DecodeMode.Scalar
  ): RequiredColumnDecoders =
    new Builder(footer, schema, decodeMode).build()

  // -- private builder --

  private final class Builder(footer: ParquetMetadata, schema: MessageType, decodeMode: DecodeMode) {
    private val cache = mutable.Map.empty[String, ColumnChunkDecoder[?]]

    def build(): RequiredColumnDecoders = decodeMode match {
      case DecodeMode.Scalar =>
        RequiredColumnDecoders(
          dictionaryLongDecoder("segment_id"),
          dictionaryLongDecoder("impressions"),
          doubleDecoder("revenue")
        )
      case DecodeMode.Vectorised =>
        RequiredColumnDecoders(
          vectorisedLongDecoder("segment_id"),
          vectorisedLongDecoder("impressions"),
          vectorisedDoubleDecoder("revenue")
        )
    }

    // -- scalar decoders --

    private def dictionaryLongDecoder(name: String): ColumnChunkDecoder[Long] =
      cached(name) {
        val desc = resolveAndValidate(name)
        val useDictPath = columnMetas(name).forall { meta =>
          new DictionaryLongColumnChunkDecoder(name, desc).supports(meta)
        }
        if (useDictPath) new DictionaryLongColumnChunkDecoder(name, desc)
        else new LongColumnChunkDecoder(name, desc)
      }

    private def doubleDecoder(name: String): ColumnChunkDecoder[Double] =
      cached(name) { new DoubleColumnChunkDecoder(name, resolveAndValidate(name)) }

    // -- vectorised decoders --

    private def vectorisedLongDecoder(name: String): ColumnChunkDecoder[Long] =
      cached(name) { new VectorisedLongColumnChunkDecoder(name, resolveAndValidate(name)) }

    private def vectorisedDoubleDecoder(name: String): ColumnChunkDecoder[Double] =
      cached(name) { new VectorisedDoubleColumnChunkDecoder(name, resolveAndValidate(name)) }

    // -- infrastructure --

    private def cached[A](name: String)(build: => ColumnChunkDecoder[A]): ColumnChunkDecoder[A] =
      cache.getOrElseUpdate(name, build).asInstanceOf[ColumnChunkDecoder[A]]

    private def resolveAndValidate(name: String): ColumnDescriptor = {
      val desc  = schema.getColumnDescription(Array(name))
      val metas = columnMetas(name)

      require(metas.nonEmpty, s"Missing footer metadata for column $name")

      metas.foreach { meta =>
        require(
          meta.getPrimitiveType == desc.getPrimitiveType,
          s"Inconsistent primitive type for $name across row groups: ${meta.getPrimitiveType} vs ${desc.getPrimitiveType}"
        )
        val unsupported = meta.getEncodings.asScala.filterNot(isSupportedEncoding)
        require(
          unsupported.isEmpty,
          s"Unsupported encodings for $name: ${unsupported.mkString(", ")}"
        )
      }
      desc
    }

    private def columnMetas(name: String): List[ColumnChunkMetaData] =
      footer.getBlocks.asScala.toList
        .flatMap(_.getColumns.asScala)
        .filter(_.getPath.toDotString == name)

    private def isSupportedEncoding(enc: Encoding): Boolean =
      SupportedLevelEncodings.contains(enc.name()) || SupportedValueEncodings.contains(enc.name())
  }
}

