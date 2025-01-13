package io.myawesome.fintech.forwarder.utils.producer

import cats.effect.*
import cats.syntax.all.*
import cats.effect.syntax.all.*
import fs2.kafka.vulcan.AvroSettings
import fs2.kafka.vulcan.SchemaRegistryClientSettings
import fs2.kafka.vulcan.avroSerializer
import fs2.kafka.KafkaProducer
import fs2.kafka.KeySerializer
import fs2.kafka.ProducerRecord
import fs2.kafka.ProducerRecords
import fs2.kafka.ProducerSettings
import fs2.kafka.ValueSerializer
import io.myawesome.fintech.avro.ClickRecord
import io.myawesome.fintech.forwarder.Codecs.given

class KafkaClickRecordProducer[F[_]: Async](topic: String, producer: KafkaProducer[F, String, ClickRecord])
    extends ClickRecordProducer[F] {
  override def send(record: ClickRecord): F[Unit] =
    producer
      .produce(
        ProducerRecords.one(
          ProducerRecord(topic, record.session_id, record),
        ),
      )
      .void
}

object KafkaClickRecordProducer {

  def make[F[_]: Async](
    topic:            String,
    bootstrapServers: String,
    schemaUrl:        String,
  ): Resource[F, ClickRecordProducer[F]] = {
    val avroSettingsF = AvroSettings {
      SchemaRegistryClientSettings[F](schemaUrl)
    }.pure[F]

    for {
      avroSettings                          <- avroSettingsF.toResource
      given ValueSerializer[F, ClickRecord] <- avroSerializer[ClickRecord].forValue(avroSettings)
      given KeySerializer[F, String]        <- avroSerializer[String].forKey(avroSettings)

      producerSettings = ProducerSettings[F, String, ClickRecord]
        .withBootstrapServers(bootstrapServers)
        .withProperty("client.id", "fintech.test")

      producer <- KafkaProducer.resource(producerSettings)

    } yield new KafkaClickRecordProducer[F](topic, producer)
  }
}
