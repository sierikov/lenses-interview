package io.myawesome.fintech.forwarder

import cats.effect.*
import io.myawesome.fintech.forwarder.forward.ForwarderService
import io.myawesome.fintech.forwarder.sink.ElasticSink
import io.myawesome.fintech.forwarder.source.KafkaDataSource
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

object Main extends IOApp.Simple {

  given logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  // TODO add proper config
  override def run: IO[Unit] = {
    val topic = "my-clicks"
    val forward = for {
      sink <- ElasticSink.make[IO](ElasticSink.Config("localhost", 9200, topic))
      source <- KafkaDataSource.make[IO](
        KafkaDataSource.Config(topic, "http://localhost:8081", "localhost:9092", "some-group-id"),
      )
      forwarder = ForwarderService.make[IO]
      _ <- forwarder.forward(source, sink).toResource
    } yield ()
    
    forward.useForever
  }
}
