package io.myawesome.fintech.forwarder

import io.myawesome.fintech.common.ClickRecord
import io.myawesome.fintech.forwarder.Codecs.given
import cats.*
import cats.effect.*
import cats.syntax.*
import fs2.*
import fs2.kafka.*
import fs2.kafka.vulcan.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import scala.concurrent.duration.DurationInt

// TODO: pass config externally
// TODO: abstract over record?
object KafkaConsumerStream {
  
  private def consumerSettings[F[_] : Sync]: ConsumerSettings[F, String, ClickRecord] = {
    val avroSettings = AvroSettings {
      SchemaRegistryClientSettings[F](Config.schemaRegistryUrl)
    }

    given Resource[F, ValueDeserializer[F, ClickRecord]] = avroDeserializer[ClickRecord].forValue(avroSettings)

    given Resource[F, KeyDeserializer[F, String]] = avroDeserializer[String].forKey(avroSettings)

    ConsumerSettings[F, String, ClickRecord]
      .withBootstrapServers(Config.bootstrapServers)
      .withGroupId(Config.kafkaGroupId)
      .withAutoOffsetReset(AutoOffsetReset.Earliest)
      .withEnableAutoCommit(false)
  }

  def consumeEvents[F[_] : Async](topic: String): Stream[F, CommittableConsumerRecord[F, String, ClickRecord]] = {
    given logger: Logger[F] = Slf4jLogger.getLogger[F]
    KafkaConsumer
      .stream(consumerSettings[F])
      .evalTap(_.subscribeTo(topic))
      .flatMap(_.stream)
  }
}

