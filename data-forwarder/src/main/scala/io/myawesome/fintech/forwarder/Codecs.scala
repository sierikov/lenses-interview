package io.myawesome.fintech.forwarder

import io.circe.Encoder
import io.circe.generic.semiauto.*
import io.myawesome.fintech.avro.ClickRecord
import org.apache.avro.Schema
import vulcan.Codec
import vulcan.generic.*

import java.util

object Codecs {
  given Codec[ClickRecord]   = Codec.derived[ClickRecord]
  given Encoder[ClickRecord] = deriveEncoder

  object SchemaCodec {
    given [T](using codecT: Codec[T]): Codec[Option[T]] =
      Codec.instance(
        schema = for {
          schemaT <- codecT.schema
          schemaNull <- Codec[None.type].schema
        } yield Schema.createUnion(util.Arrays.asList(schemaT, schemaNull)),
        encode = {
          case Some(value) => codecT.encode(value)
          case None => Right(null)
        },
        decode = { (value, schema) =>
          value match {
            case null => Right(None)
            case _ => codecT.decode(value, schema).map(Some(_))
          }
        },

      )
  }
}
