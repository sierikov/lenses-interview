package io.myawesome.fintech.forwarder.forward

import cats.effect.*
import cats.syntax.all.*
import cats.effect.syntax.all.*
import io.myawesome.fintech.avro.ClickRecord
import io.myawesome.fintech.forwarder.source.DataRecord
import io.myawesome.fintech.forwarder.source.Source
import io.myawesome.fintech.forwarder.sink.Sink
import fs2.Stream
import fs2.Chunk
import fs2.kafka.CommittableOffsetBatch
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.DurationInt

class ForwarderService[F[_]: Async](
  using
  logger: Logger[F],
) extends Forwarder[F, String, ClickRecord] {
  
  def forward(
    source: Source[F, String, ClickRecord],
    sink:   Sink[F, ClickRecord],
  ): F[Unit] =
    source
      .consume
      .parEvalMapUnbounded(p => processPartition(p, sink))
      .compile
      .drain

  private def processPartition(
    partitionStream: Stream[F, DataRecord[F, String, ClickRecord]],
    sink:            Sink[F, ClickRecord],
  ) = partitionStream
    .groupWithin(10, 20.seconds)
    .evalMap(c => if (c.isEmpty) Concurrent[F].unit else processChunk(c, sink))
    .compile
    .drain

  private def processChunk(chunk: Chunk[DataRecord[F, String, ClickRecord]], sink: Sink[F, ClickRecord]): F[Unit] = {
    val offsetBatch = CommittableOffsetBatch.fromFoldable(chunk.map(_.offset))
    val events      = chunk.map(rec => (rec.timestamp, rec.value)).toList
    logger.info(s"Forwarding batch. Partition ${chunk.head.get.partition} size ${events.size}") *>
      sink.sendBatch(events) *>
      offsetBatch.commit
  }

}

object ForwarderService {
  def make[F[_]:         Async: Logger]: ForwarderService[F]              = new ForwarderService[F]
  def makeResource[F[_]: Async: Logger]: Resource[F, ForwarderService[F]] = make[F].pure[F].toResource
}
