package io.myawesome.fintech.forwarder.forward

import cats.effect.*
import cats.effect.kernel.Ref
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all.*
import fs2.Stream
import fs2.kafka.CommittableOffset
import io.myawesome.fintech.avro.ClickRecord
import io.myawesome.fintech.forwarder.forward.ForwarderLogicSpec.SinkMock
import io.myawesome.fintech.forwarder.forward.ForwarderLogicSpec.SourceMock
import io.myawesome.fintech.forwarder.sink.Sink
import io.myawesome.fintech.forwarder.source.DataRecord
import io.myawesome.fintech.forwarder.source.Source
import io.myawesome.fintech.forwarder.utils.generator.RandomClickRecordGenerator
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import java.time.Instant
import scala.concurrent.duration._

class ForwarderLogicSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  "ForwarderService" - {
    "should forward messages preserving order within each partition" in {
      for {
        bigStream       <- generateRecords(partitions = 2, size = List(2, 3))
        sourceMock       = new SourceMock[IO](bigStream)
        receivedBatches <- Ref.of[IO, Map[Int, List[(Instant, ClickRecord)]]](Map.empty)
        sinkMock         = new SinkMock[IO](receivedBatches)
        forwarder        = createForwarderService()
        _               <- forwarder.forward(sourceMock, sinkMock)
        batches         <- receivedBatches.get
        _ <- validateBatches(
          batches,
          expectedPartitions = Map(
            1 -> bigStream.head.map(_.value),
            2 -> bigStream.last.map(_.value),
          ),
        )
      } yield ()
    }

    "should maintain order when source emits streams of streams" in {
      for {
        bigStream       <- generateRecords(partitions = 2, size = List(2, 3))
        sourceMock       = new SourceMock[IO](bigStream)
        receivedBatches <- Ref.of[IO, Map[Int, List[(Instant, ClickRecord)]]](Map.empty)
        sinkMock         = new SinkMock[IO](receivedBatches)
        forwarder        = createForwarderService()
        _               <- forwarder.forward(sourceMock, sinkMock)
        batches         <- receivedBatches.get
        _ <- validateBatches(
          batches,
          expectedPartitions = Map(
            1 -> bigStream.head.map(_.value),
            2 -> bigStream.last.map(_.value),
          ),
        )
      } yield ()
    }
  }

  /** Helper method to create ForwarderService with common configurations */
  private def createForwarderService(): ForwarderService[IO] = new ForwarderService[IO](
    batchSize    = 2,
    batchTimeout = 1.seconds,
  )

  /** Generates records for a given number of partitions and amount of messages per partition.
    *
    * @param partitions
    *   Number of partitions.
    * @param size
    *   List containing the amount of records per partition.
    * @return
    *   A list containing lists of DataRecords per partition.
    */
  private def generateRecords(
    partitions: Int,
    size:       List[Int],
  ): IO[List[List[DataRecord[IO, String, ClickRecord]]]] = {
    require(partitions == size.size, "Partitions and counts size must match")
    val generator    = RandomClickRecordGenerator.make[IO]
    val startInstant = Instant.parse("2025-01-01T01:01:01Z")
    for {
      records <- size
        .zipWithIndex
        .traverse { case (count, idx) =>
          generator
            .generate(count)
            .map { records =>
              records.zipWithIndex.map { case (rec, i) =>
                val partition = idx + 1
                // Embed partition number into the 'id' of ClickRecord
                val updatedRec = rec.copy(session_id = s"partition-$partition-id") // only for test purposes
                DataRecord[IO, String, ClickRecord](
                  key       = s"key$partition",
                  value     = updatedRec,
                  partition = partition,
                  timestamp = startInstant.plusSeconds(idx * 10 + i + 1),
                  offset    = fakeOffset[IO](partition = partition),
                )
              }
            }
        }
    } yield records
  }

  /** Validates that the received batches match the expected records per partition.
    *
    * @param batches
    *   Received batches from SinkMock.
    * @param expectedPartitions
    *   Map of partition number to expected list of ClickRecords.
    */
  private def validateBatches(
    batches:            Map[Int, List[(Instant, ClickRecord)]],
    expectedPartitions: Map[Int, List[ClickRecord]],
  ): IO[Unit] = expectedPartitions.toList.traverse_ {
    case (partition, expectedRecords) =>
      IO {
        val actualRecords = batches.getOrElse(partition, List()).map(_._2)
        actualRecords should contain theSameElementsInOrderAs expectedRecords
      }
  }

  def fakeOffset[F[_]: Sync](
    partition: Int,
    topic:     String = "dummy-topic",
    offset:    Long   = 0L,
  ): CommittableOffset[F] = CommittableOffset.apply[F](
    topicPartition    = new TopicPartition(topic, partition),
    offsetAndMetadata = new OffsetAndMetadata(offset),
    consumerGroupId   = Some("some-testing-test"),
    commit            = _ => Sync[F].unit,
  )
}

object ForwarderLogicSpec {
  class SinkMock[F[_]: Sync](receivedBatches: Ref[F, Map[Int, List[(Instant, ClickRecord)]]])
      extends Sink[F, ClickRecord] {

    override def sendBatch(events: List[(Instant, ClickRecord)]): F[Unit] =
      events.headOption match {
        case Some((_, clickRecord)) =>
          // Extract partition number from ClickRecord.id (assumes format "partition-X-UUID")
          val partitionIdPattern = """partition-(\d+)-.*""".r
          val partition = clickRecord.session_id match {
            case partitionIdPattern(id) => id.toInt
            case _                      => -1 // Handle parsing error if necessary
          }
          receivedBatches.update { batches =>
            val updatedEvents = batches.getOrElse(partition, List()) ++ events
            batches.updated(partition, updatedEvents)
          }
        case None => Sync[F].unit // No events to process
      }
  }

  class SourceMock[F[_]: Sync](
    partitions: List[List[DataRecord[F, String, ClickRecord]]],
  ) extends Source[F, String, ClickRecord] {
    override def consume: Stream[F, Stream[F, DataRecord[F, String, ClickRecord]]] =
      Stream.emits(partitions.map(Stream.emits))
  }
}
