package io.myawesome.fintech.forwarder.forward

import cats.effect.*
import cats.syntax.all.*
import io.myawesome.fintech.avro.ClickRecord
import io.myawesome.fintech.forwarder.source.Source
import io.myawesome.fintech.forwarder.sink.Sink
import fs2.Stream
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.DurationInt

import java.time.Instant

class ForwarderService[F[_]: Async](
  source: Source[F, String, ClickRecord],
  sink:   Sink[F, ClickRecord],
)(
  using
  logger: Logger[F],
) extends Forwarder[F, String, ClickRecord] {

  def forward(topic: String): Stream[F, Unit] =
    source
      .consume(topic)
      .parEvalMapUnbounded { partitionStream =>
        partitionStream
          .groupWithin(10, 20.seconds)
          .evalMap { chunkOfCommits =>
            if (chunkOfCommits.isEmpty) Concurrent[F].unit
            else {
              val events = chunkOfCommits.map { commitable =>
                val event     = commitable.record.value
                val partition = commitable.record.partition
                val offset    = commitable.record.offset
                val timestamp = Instant.ofEpochMilli(
                  commitable.record.timestamp.createTime.get, // assume there is always timestamp
                )
                (timestamp, event)
              }.toList

              logger.info(
                s"Processing batch for partition ${chunkOfCommits.head.get.record.partition} of size ${events.size}",
              )
                *> sink.sendBatch(events)
                *> chunkOfCommits.toList.traverse_(_.offset.commit)
            }
          }
          .compile
          .drain
      }
}

object ForwarderService {
  def make[F[_]: Async: Logger](
    source: Source[F, String, ClickRecord],
    sink:   Sink[F, ClickRecord],
  ): ForwarderService[F] =
    new ForwarderService[F](source, sink)
}
