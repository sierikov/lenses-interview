package io.myawesome.fintech.forwarder

import io.circe.Encoder
import io.circe.generic.semiauto.*
import io.myawesome.fintech.common.ClickRecord
import vulcan.Codec
import vulcan.generic.*

object Codecs {
  given Codec[ClickRecord] = Codec.derived[ClickRecord]
  
  given Encoder[ClickRecord] = deriveEncoder
}
