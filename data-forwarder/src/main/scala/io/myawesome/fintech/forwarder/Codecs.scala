package io.myawesome.fintech.forwarder

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.*
import io.myawesome.fintech.avro.ClickRecord
import org.apache.avro.Schema
import vulcan.Codec
import vulcan.generic.*

import java.util

object Codecs {
  given Codec[ClickRecord]   = Codec.derived[ClickRecord]
  given Encoder[ClickRecord] = deriveEncoder
  given Decoder[ClickRecord] = deriveDecoder
}
