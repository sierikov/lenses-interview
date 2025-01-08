package io.myawesome.fintech.generator

import cats.effect.{IO, IOApp}
import cats.effect.std.Random
import fs2.Stream
import scala.concurrent.duration.DurationInt

object Main extends IOApp.Simple {

  override def run: IO[Unit] = program

  private def program: IO[Unit] = {
    for {
      random <- Random.scalaUtilRandom[IO]
      given Random[IO] = random
      generator = RandomClickRecordGenerator.makeLimited[IO]
      _ <- stream(generator).compile.drain
    } yield ()
  }

  private def stream(generator: RandomClickRecordGenerator[IO]): Stream[IO, Unit] = {
    Stream.awakeEvery[IO](5.seconds)
      .evalMap { _ =>
        generator.generate.flatMap { record =>
          IO.println(s"Generated ClickRecord: $record")
        }
      }
  }
}