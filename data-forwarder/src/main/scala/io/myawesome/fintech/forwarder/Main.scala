package io.myawesome.fintech.forwarder

import io.myawesome.fintech.common.ClickRecord
import cats.effect.*
import fs2.*
import fs2.kafka.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.DurationInt

object Main extends IOApp.Simple {

  given logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  // TODO: Move to ES client
//  val retryPolicy = RetryPolicies.limitRetries[IO](5) join
//    RetryPolicies.exponentialBackoff(1.second)

  override def run: IO[Unit] =

    ElasticClient.make[IO](Config.elasticHost, Config.elasticPort).use { esClient =>
      KafkaConsumerStream
        .consumeEvents[IO](Config.kafkaEventTopic)
        .evalMap { committableRec =>
          val record = committableRec.record
          val event: ClickRecord = record.value

          logger.info(s"Processing $event at offset: ${committableRec.offset}") *>
          esClient.sendEvent(event).as(committableRec.offset)
        }
        .through(commitBatchWithin(100, 15.seconds))
        .compile
        .drain
    }
}
