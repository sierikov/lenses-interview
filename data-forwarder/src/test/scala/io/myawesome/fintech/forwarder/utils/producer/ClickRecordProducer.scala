package io.myawesome.fintech.forwarder.utils.producer

import cats.effect.*
import cats.syntax.all.*
import fs2.kafka.*
import fs2.kafka.vulcan.{AvroSettings, SchemaRegistryClientSettings, avroDeserializer, avroSerializer}
import io.myawesome.fintech.avro.ClickRecord
import io.myawesome.fintech.forwarder.Codecs.given

/** A simple trait for sending ClickRecord messages.
  */
trait ClickRecordProducer[F[_]] {

  /** Sends a single ClickRecord.
    */
  def send(record: ClickRecord): F[Unit]
}
