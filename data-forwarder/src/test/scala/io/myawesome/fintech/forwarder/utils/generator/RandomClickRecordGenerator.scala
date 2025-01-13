package io.myawesome.fintech.forwarder.utils.generator

import cats.*
import cats.effect.std.Random
import cats.syntax.all.*
import io.myawesome.fintech.avro.ClickRecord

class RandomClickRecordGenerator[F[_]: Monad] extends ClickRecordGenerator[F] {

  override def generateOne: F[ClickRecord] = ClickRecordGen.clickRecordGen.sample.get.pure[F]
  
}

object RandomClickRecordGenerator {
  def make[F[_]: Monad]: ClickRecordGenerator[F] = RandomClickRecordGenerator[F]
}
