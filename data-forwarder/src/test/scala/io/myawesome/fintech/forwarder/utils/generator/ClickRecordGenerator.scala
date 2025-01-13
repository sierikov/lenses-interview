package io.myawesome.fintech.forwarder.utils.generator

import cats.*
import cats.effect.*
import cats.syntax.all.*
import cats.effect.std.Random
import io.myawesome.fintech.avro.ClickRecord

trait ClickRecordGenerator[F[_]: Applicative] {
  def generateOne: F[ClickRecord]

  def generate(n: Int): F[List[ClickRecord]] = (0 to n).map(_ => generateOne).toList.sequence
}

object ClickRecordGenerator {
  def apply[F[_]: Applicative](using
      ev: ClickRecordGenerator[F]
  ): ClickRecordGenerator[F] = ev
}
