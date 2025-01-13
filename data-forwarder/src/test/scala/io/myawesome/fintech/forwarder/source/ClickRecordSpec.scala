package io.myawesome.fintech.forwarder.source

import io.myawesome.fintech.avro.ClickRecord
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ClickRecordSpec extends AnyFlatSpec with Matchers {

  "ClickRecord" should "generate a usable ClickRecord instance" in {
    val record = ClickRecord(
      session_id = "scala3",
      browser    = Some("Chrome"),
      campaign   = None,
      channel    = "direct",
      referrer   = Some("example.com"),
      ip         = None,
    )

    record.session_id shouldBe "scala3"
    record.browser shouldBe Some("Chrome")
  }
}
