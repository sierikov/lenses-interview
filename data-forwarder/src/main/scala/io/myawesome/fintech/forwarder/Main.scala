package io.myawesome.fintech.forwarder

import cats.syntax.all.*
import cats.effect.*
import io.myawesome.fintech.forwarder.forward.ForwarderService
import io.myawesome.fintech.forwarder.sink.ElasticSink
import io.myawesome.fintech.forwarder.source.KafkaDataSource
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.ConfigSource

object Main extends IOApp.Simple {

  given logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  override def run: IO[Unit] = {
    val forward = for {
      config   <- ConfigSource.default.loadOrThrow[Config].pure[IO].toResource
      sink     <- ElasticSink.make[IO](config.sinkConfig)
      source   <- KafkaDataSource.make[IO](config.sourceConfig)
      forwarder = ForwarderService.make[IO](config.forwarderConfig)
      _        <- forwarder.forward(source, sink).toResource
    } yield ()

    forward.useForever
  }
}
