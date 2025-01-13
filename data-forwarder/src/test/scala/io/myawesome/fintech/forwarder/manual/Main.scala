package io.myawesome.fintech.forwarder.manual

import cats.effect.*
import cats.effect.std.Random
import cats.syntax.all.*
import fs2.Stream
import io.myawesome.fintech.forwarder.utils.generator.ClickRecordGenerator
import io.myawesome.fintech.forwarder.utils.generator.LimitedRandomClickRecordGenerator
import io.myawesome.fintech.forwarder.utils.producer.ClickRecordProducer
import io.myawesome.fintech.forwarder.utils.producer.KafkaClickRecordProducer
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.*

object Main extends IOApp.Simple {

  given logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  override def run: IO[Unit] = program.useForever

  private def program: Resource[IO, Unit] = for {
    random          <- Random.scalaUtilRandom[IO].toResource
    given Random[IO] = random
    generator       <- LimitedRandomClickRecordGenerator.make[IO].pure[IO].toResource
    producer <- KafkaClickRecordProducer.make[IO](
      ManualConfig.kafkaEventTopic,
      ManualConfig.bootstrapServers,
      ManualConfig.schemaRegistryUrl,
    )
    _ <- stream(producer, generator).compile.drain.toResource
  } yield ()

  private def stream(
    producer:  ClickRecordProducer[IO],
    generator: ClickRecordGenerator[IO],
  ): Stream[IO, Unit] =
    Stream
      .awakeEvery[IO](ManualConfig.period)
      .evalMap { _ =>
        generator.generateOne.flatMap { record =>
          logger.info(s"Produced $record") *> producer.send(record)
        }
      }
      .drain
}
