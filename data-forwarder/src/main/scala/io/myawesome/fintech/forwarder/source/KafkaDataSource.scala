package io.myawesome.fintech.forwarder.source

import cats.effect.*
import cats.syntax.all.*
import cats.effect.syntax.all.*
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

import java.time.Instant

final class KafkaDataSource[F[_]: Async](
  topic:    String,
  consumer: KafkaConsumer[F, String, ClickRecord],
) extends Source[F, String, ClickRecord] {
  given logger: Logger[F] = Slf4jLogger.getLogger[F]

  override def consume: Stream[F, Stream[F, DataRecord[F, String, ClickRecord]]] =
    Stream.eval(consumer.subscribeTo(topic)) >>
      consumer.partitionedStream.map { partitionStream =>
        partitionStream.map { committableRecord =>
          val cr = committableRecord.record
          DataRecord(
            key       = cr.key,
            value     = cr.value,
            partition = cr.partition,
            timestamp = cr.timestamp.createTime.map(Instant.ofEpochMilli).get,
            offset    = committableRecord.offset,
          )
        }
      }
}

object KafkaDataSource {

  final case class Config(topic: String, schemaRegistryUrl: String, bootstrapServers: String, kafkaGroupId: String)

  def make[F[_]: Async](config: Config): Resource[F, KafkaDataSource[F]] = {
    val avroSettingsF = AvroSettings {
      SchemaRegistryClientSettings[F](config.schemaRegistryUrl)
    }.pure[F]

    for {
      avroSettings <- avroSettingsF.toResource

      given ValueDeserializer[F, ClickRecord] <- avroDeserializer[ClickRecord].forValue(avroSettings)
      given KeyDeserializer[F, String]        <- avroDeserializer[String].forKey(avroSettings)

      consumerSettings = ConsumerSettings[F, String, ClickRecord]
        .withBootstrapServers(config.bootstrapServers)
        .withGroupId(config.kafkaGroupId)
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withEnableAutoCommit(false)

      consumer <- KafkaConsumer.resource(consumerSettings)

    } yield new KafkaDataSource[F](config.topic, consumer)
  }
}
