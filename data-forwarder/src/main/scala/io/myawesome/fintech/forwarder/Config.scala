package io.myawesome.fintech.forwarder

import io.myawesome.fintech.forwarder.forward.ForwarderService
import io.myawesome.fintech.forwarder.sink.ElasticSink
import io.myawesome.fintech.forwarder.source.KafkaDataSource
import pureconfig.*

case class Config(
  sourceConfig:    KafkaDataSource.Config,
  sinkConfig:      ElasticSink.Config,
  forwarderConfig: ForwarderService.Config,
) derives ConfigReader
