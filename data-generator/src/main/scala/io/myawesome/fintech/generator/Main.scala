package io.myawesome.fintech.generator

import cats.effect.{IO, IOApp}
import cats.effect.std.Random
import fs2.Stream
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.DurationInt

object Main extends IOApp.Simple {

  given logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  override def run: IO[Unit] = program

  private def program: IO[Unit] = {
    for {
      random <- Random.scalaUtilRandom[IO]
      given Random[IO] = random
      _ <- logger.info("Random init done")
      generator = RandomClickRecordGenerator.makeLimited[IO]
      _ <- stream(generator).compile.drain
    } yield ()
  }

  private def stream(generator: RandomClickRecordGenerator[IO]): Stream[IO, Unit] = {
    Stream.awakeEvery[IO](5.seconds)
      .evalMap { _ =>
        generator.generate.flatMap { record =>
          logger.info(s"Generated ClickRecord: $record")
        }
      }
  }
}