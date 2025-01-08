package io.myawesome.fintech.generator

import cats.effect.std.Random
import cats.*
import io.myawesome.fintech.common.ClickRecord

trait RandomClickRecordGenerator[F[_]] {
  def generate: F[ClickRecord]
}

object RandomClickRecordGenerator {
  def apply[F[_]](using ev: RandomClickRecordGenerator[F]): RandomClickRecordGenerator[F] = ev
  def makeLimited[F[_]: Monad: Random]: RandomClickRecordGenerator[F] = LimitedRandomClickRecordGenerator[F]
}



