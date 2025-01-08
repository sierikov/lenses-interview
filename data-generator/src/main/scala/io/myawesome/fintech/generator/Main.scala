package io.myawesome.fintech.generator

import cats.effect.{IO, IOApp}
import cats.effect.std.Random

object Main extends IOApp.Simple {

  override def run: IO[Unit] = program

  private def program: IO[Unit] = {
    for {
      random <- Random.scalaUtilRandom[IO]
      given Random[IO] = random
      generator = RandomClickRecordGenerator.makeLimited[IO]
      clickRecord <- generator.generate
      _ <- IO.println(s"Generated ClickRecord: $clickRecord")
    } yield ()
  }
}