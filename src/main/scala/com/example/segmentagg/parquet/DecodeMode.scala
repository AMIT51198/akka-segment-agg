package com.example.segmentagg.parquet

/**
 * Selects between scalar (row-at-a-time) and vectorised (batch-at-a-time) Parquet decoding.
 *
 * Sealed ADT — all subtypes are defined here, ensuring exhaustive pattern matching at compile time.
 */
sealed abstract class DecodeMode(private val label: String) extends Product with Serializable {
  override def toString: String = label
}

object DecodeMode {
  case object Scalar     extends DecodeMode("scalar")
  case object Vectorised extends DecodeMode("vectorised")

  /** Parse a user-supplied string into a [[DecodeMode]], or fail with a clear error. */
  def fromString(s: String): DecodeMode = s.toLowerCase match {
    case "scalar"                    => Scalar
    case "vectorised" | "vectorized" => Vectorised
    case other =>
      throw new IllegalArgumentException(
        s"Unknown decode mode: '$other' (expected 'scalar' or 'vectorised')"
      )
  }
}

