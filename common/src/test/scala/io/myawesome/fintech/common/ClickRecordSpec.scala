package io.myawesome.fintech.common

import io.myawesome.fintech.common.VulcanUnionTypeOrderEnforcer.given
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.*
import vulcan.Codec
import vulcan.generic.given

import scala.io.Source

// can be abstracted further
class ClickRecordSpec extends AnyFlatSpec with Matchers {
  it should "generated usable ClickRecord" in {
    val record = ClickRecord(
      session_id = "scala3", // the camel case was left out of scope
      browser = Some("Chrome"),
      campaign = None,
      channel = "direct",
      referrer = Some("example.com"),
      ip = None // can be refined
    )

    record.session_id shouldBe "scala3"
    record.browser shouldBe Some("Chrome")
  }

  it should "match the Avro schema in /src/main/avro" in {
    val stream = Source.fromFile("common/src/main/avro/ClickRecord.avsc")
    val expectedSchema = stream.getLines().mkString("").replaceAll("\\s+", "")
    val generatedSchema = Codec.derived[ClickRecord].schema.getOrElse("").toString

    generatedSchema shouldBe expectedSchema
  }
}
