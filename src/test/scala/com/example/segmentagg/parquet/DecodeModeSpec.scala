package com.example.segmentagg.parquet

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Tests for [[DecodeMode]] sealed ADT and parsing.
 */
class DecodeModeSpec extends AnyFunSuite with Matchers {

  test("fromString parses 'scalar'") {
    DecodeMode.fromString("scalar") shouldBe DecodeMode.Scalar
  }

  test("fromString parses 'vectorised'") {
    DecodeMode.fromString("vectorised") shouldBe DecodeMode.Vectorised
  }

  test("fromString parses 'vectorized' (American spelling)") {
    DecodeMode.fromString("vectorized") shouldBe DecodeMode.Vectorised
  }

  test("fromString is case-insensitive") {
    DecodeMode.fromString("SCALAR") shouldBe DecodeMode.Scalar
    DecodeMode.fromString("Vectorised") shouldBe DecodeMode.Vectorised
    DecodeMode.fromString("VECTORIZED") shouldBe DecodeMode.Vectorised
  }

  test("fromString rejects unknown mode") {
    val ex = intercept[IllegalArgumentException] {
      DecodeMode.fromString("turbo")
    }
    ex.getMessage should include("turbo")
  }

  test("toString returns lowercase label") {
    DecodeMode.Scalar.toString shouldBe "scalar"
    DecodeMode.Vectorised.toString shouldBe "vectorised"
  }
}

