package io.myawesome.fintech.forwarder

// TODO: make with pureconfig
object Config {
  val bootstrapServers = "localhost:9092"
  val schemaRegistryUrl = "http://localhost:8081"
  val elasticHost = "localhost"
  val elasticPort = 9200
  
  val kafkaGroupId = "forwarder-group"
  val kafkaEventTopic = "my-clicks"
}
