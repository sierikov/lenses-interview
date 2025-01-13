package io.myawesome.fintech.forwarder.manual

import scala.concurrent.duration.DurationInt

object ManualConfig {
  val bootstrapServers  = "localhost:9092"
  val schemaRegistryUrl = "http://localhost:8081"
  val kafkaEventTopic   = "my-clicks"
  val period            = 2.seconds
}
