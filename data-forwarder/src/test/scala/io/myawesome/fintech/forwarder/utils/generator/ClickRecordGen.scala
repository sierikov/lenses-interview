package io.myawesome.fintech.forwarder.utils.generator

import io.myawesome.fintech.avro.ClickRecord
import org.scalacheck.Gen

object ClickRecordGen {
  val uuidGen: Gen[String] = Gen.uuid.map(_.toString)
  
  val ipGenOpt: Gen[Option[String]] = Gen.option {
    for {
      a <- Gen.choose(0, 255)
      b <- Gen.choose(0, 255)
      c <- Gen.choose(0, 255)
      d <- Gen.choose(0, 255)
    } yield s"$a.$b.$c.$d"
  }

  def randomString(maxSize: Int): Gen[String] = for {
    length <- Gen.choose(5, maxSize)
    chars <- Gen.listOfN(length, Gen.alphaNumChar)
  } yield chars.mkString
  
  val clickRecordGen: Gen[ClickRecord] = for {
    sessionId <- uuidGen
    browser   <- Gen.option(randomString(20))
    campaign  <- Gen.option(randomString(40))
    channel   <- randomString(15)
    referrer  <- Gen.option(randomString(40))
    ip        <- ipGenOpt
  } yield ClickRecord(sessionId, browser, campaign, channel, referrer, ip)
}
