package io.myawesome.fintech.forwarder.source

import cats.effect.*
import cats.syntax.all.*
import vulcan.Codec
import cats.effect.Async
import cats.effect.Resource
import fs2.Stream
import fs2.kafka.*
import fs2.kafka.vulcan.*
import io.myawesome.fintech.avro.ClickRecord
import io.myawesome.fintech.forwarder.Codecs.given
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

final class KafkaDataSource[F[_]: Async](
  consumer: KafkaConsumer[F, String, ClickRecord],
) extends Source[F, String, ClickRecord] {
  given logger: Logger[F] = Slf4jLogger.getLogger[F]

  override def consume(topic: String): Stream[F, Stream[F, CommittableConsumerRecord[F, String, ClickRecord]]] =
    Stream.eval(consumer.subscribeTo(topic)) >> consumer.partitionedStream
}

object KafkaDataSource {

  final case class Config(schemaRegistryUrl: String, bootstrapServers: String, kafkaGroupId: String)

  def make[F[_]: Async](config: Config): Resource[F, KafkaDataSource[F]] = {
    val avroSettingsF = AvroSettings {
      SchemaRegistryClientSettings[F](config.schemaRegistryUrl)
    }.pure[F]

    for {
      avroSettings <- Resource.eval(avroSettingsF)

      given ValueDeserializer[F, ClickRecord] <- avroDeserializer[ClickRecord].forValue(avroSettings)
      given KeyDeserializer[F, String]        <- avroDeserializer[String].forKey(avroSettings)

      consumerSettings = ConsumerSettings[F, String, ClickRecord]
        .withBootstrapServers(config.bootstrapServers)
        .withGroupId(config.kafkaGroupId)
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withEnableAutoCommit(false)

      consumer <- KafkaConsumer.resource(consumerSettings)

    } yield new KafkaDataSource[F](consumer)
  }
}
