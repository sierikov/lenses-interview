package io.myawesome.fintech.generator


import cats.effect.*
import cats.effect.std.Random
import cats.syntax.all.*
import vulcan.Codec
import vulcan.generic.*
import fs2.Stream
import fs2.kafka.*
import fs2.kafka.vulcan.*
import io.myawesome.fintech.common.ClickRecord
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.*

object Main extends IOApp.Simple {

  given logger: Logger[IO] = Slf4jLogger.getLogger[IO]


  private val avroSettings = AvroSettings {
    SchemaRegistryClientSettings[IO](Config.schemaRegistryUrl)
  }

  import io.myawesome.fintech.common.VulcanUnionTypeOrderEnforcer.*

  given Codec[ClickRecord] = Codec.derived[ClickRecord]

  given Resource[IO, ValueSerializer[IO, ClickRecord]] = avroSerializer[ClickRecord].forValue(avroSettings)

  given Resource[IO, KeySerializer[IO, String]] = avroSerializer[String].forKey(avroSettings)

  private val producerSettings = ProducerSettings[IO, String, ClickRecord]
    .withBootstrapServers(Config.bootstrapServers)
    .withProperty("client.id", "fintech.generator")

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
    KafkaProducer.stream(producerSettings).flatMap { producer =>
        Stream.awakeEvery[IO](5.seconds).evalMap { _ =>
          generator.generate.flatMap { record =>
            logger.info(s"Produced $record") *>
              producer.produce(
                ProducerRecords.one(ProducerRecord(Config.kafkaEventTopic, record.session_id, record))
              )
          }
        }.drain
      }
      .parJoinUnbounded
      .handleErrorWith { case ex: Exception => Stream.emit(logger.error(s"Failed to send record! Error: ${ex.getMessage}")) }
  }
}