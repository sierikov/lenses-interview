package io.myawesome.fintech.common

import vulcan.Codec
import vulcan.generic._

object Main extends App {
    val schema = Codec.derived[ClickRecord]
    println(schema.schema.getOrElse("").toString)
}