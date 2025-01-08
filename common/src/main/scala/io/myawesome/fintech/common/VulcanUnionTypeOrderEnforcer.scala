package io.myawesome.fintech.common

import vulcan.Codec
import org.apache.avro.Schema as AvroSchema
import java.util

object VulcanUnionTypeOrderEnforcer {
  given [T](using codecT: Codec[T]): Codec[Option[T]] =
    Codec.instance(
      schema = for {
        schemaT <- codecT.schema
        schemaNull <- Codec[None.type].schema
      } yield AvroSchema.createUnion(util.Arrays.asList(schemaT, schemaNull)),
      encode = {
        case Some(value) => codecT.encode(value)
        case None => Right(null)
      },
      decode = { (value, schema) =>
        value match {
          case null => Right(None)
          case _ => codecT.decode(value, schema).map(Some(_))
        }
      }
    )
}
