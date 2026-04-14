package com.example.segmentagg.parquet

import org.apache.parquet.bytes.{BytesInput, BytesUtils}
import org.apache.parquet.column.page.{DictionaryPage, PageReader}
import org.apache.parquet.column.values.ValuesReader
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder
import org.apache.parquet.column.{ColumnDescriptor, Dictionary, Encoding, ValuesType}
import org.apache.parquet.io.ParquetDecodingException

/**
 * Shared Parquet page-level helper methods used by both scalar and vectorised
 * column chunk decoders. Eliminates the duplication of dictionary init,
 * RLE iterator creation, and ValuesReader construction.
 */
private[parquet] trait ParquetPageHelpers {

  def descriptor: ColumnDescriptor
  def columnName: String

  protected final def readDictionary(pageReader: PageReader): Option[Dictionary] =
    Option(pageReader.readDictionaryPage()).map(initDictionary)

  protected final def initDictionary(page: DictionaryPage): Dictionary =
    page.getEncoding.initDictionary(descriptor, page)

  protected final def valuesReader(
      encoding: Encoding,
      valuesType: ValuesType,
      dictionary: Option[Dictionary]
  ): ValuesReader =
    if (encoding.usesDictionary) {
      val d = dictionary.getOrElse {
        throw new ParquetDecodingException(s"Missing dictionary for $columnName and encoding $encoding")
      }
      encoding.getDictionaryBasedValuesReader(descriptor, valuesType, d)
    } else {
      encoding.getValuesReader(descriptor, valuesType)
    }

  protected final def rleIterator(maxLevel: Int, bytes: BytesInput): IntIterator =
    if (maxLevel == 0) ZeroIntIterator
    else new RleIntIterator(
      new RunLengthBitPackingHybridDecoder(BytesUtils.getWidthFromMaxInt(maxLevel), bytes.toInputStream)
    )

  /** Validate that the column is required (non-null, non-repeated) and reject otherwise. */
  protected final def requireNonRepeatedNonNull(repetitionLevel: Int, definitionLevel: Int): Unit = {
    if (repetitionLevel != 0)
      throw new IllegalArgumentException(s"Unsupported repeated Parquet field: $columnName")
    if (definitionLevel < descriptor.getMaxDefinitionLevel)
      throw new IllegalArgumentException(s"Null values are not supported for $columnName")
  }
}

