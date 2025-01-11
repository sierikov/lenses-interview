package io.myawesome.fintech.forwarder

import cats.effect.*
import io.myawesome.fintech.forwarder.forward.ForwarderService
import io.myawesome.fintech.forwarder.sink.ElasticSink
import io.myawesome.fintech.forwarder.source.KafkaDataSource
import fs2.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

object Main extends IOApp.Simple {

  given logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  // TODO add proper config
  override def run: IO[Unit] = {
    val topic = "my-clicks"
    val forwarder = for {
      sink <- ElasticSink.make[IO](ElasticSink.Config("localhost", 9200, topic))
      source <- KafkaDataSource.make[IO](
        KafkaDataSource.Config("http://localhost:8081", "localhost:9092", "some-group-id"),
      )
    } yield ForwarderService.make[IO](source, sink)

    forwarder.use(f => f.forward(topic).compile.drain)
  }
}
