package io.myawesome.fintech.forwarder.sink

import cats.effect.*
import cats.syntax.all.*
import io.circe.syntax.*
import io.myawesome.fintech.avro.ClickRecord
import io.myawesome.fintech.forwarder.Codecs.given
import io.myawesome.fintech.forwarder.sink.ElasticSink.createIndex
import org.http4s.*
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder
import org.typelevel.log4cats.Logger

import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import scala.concurrent.duration.DurationInt

final class ElasticSink[F[_]: Async](
  client: Client[F],
  config: ElasticSink.Config,
)(
  using
  clock:  Clock[F],
  logger: Logger[F],
) extends Sink[F, ClickRecord] {

  // 1 request per index
  def sendBatch(events: List[(Instant, ClickRecord)]): F[Unit] =
    events
      .groupBy { case (timestamp, _) =>
        createIndex(config.indexName, timestamp)
      }
      .toList
      .traverse_ { case (index, events) =>
        val records = events.map(_._2)
        val req     = createBulkRequest(index, records)
        client.run(req).use(handleBulkResponse)
      }

  private def createBulkRequest(index: String, records: List[ClickRecord]): Request[F] = {
    val lines = records.flatMap { record =>
      val actionLine = s"""{"index":{"_index":"$index"}}"""
      val sourceLine = record.asJson.noSpaces
      List(actionLine, sourceLine)
    }

    val requestBody = lines.mkString("\n") + "\n"

    val uri = Uri.unsafeFromString(s"http://${config.host}:${config.port}/_bulk")
    Request[F](Method.POST, uri)
      .withEntity(requestBody)
      .putHeaders(Header("Content-Type", "application/json"))
  }

  private def handleBulkResponse(resp: Response[F]): F[Unit] =
    if (resp.status.isSuccess)
      logger.info("Successfully send a batch for index!") *> Async[F].unit
    else
      Async[F].raiseError(
        new RuntimeException(s"Failed to index documents. Status: ${resp.status}"),
      )
}

object ElasticSink {

  final case class Config(
    host:      String,
    port:      Int,
    indexName: String,
  )

  def createIndex(index: String, time: Instant): String = {
    val zonedTime   = time.atZone(ZoneId.of("UTC"))
    val minutesSlot = (zonedTime.getMinute / 15) * 15
    val roundedTime = zonedTime
      .withMinute(minutesSlot)
      .withSecond(0)
      .withNano(0)

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm")
    s"$index-${roundedTime.format(formatter)}"
  }

  def make[F[_]: Async: Logger](config: Config): Resource[F, ElasticSink[F]] =
    EmberClientBuilder
      .default[F]
      .withTimeout(30.seconds)
      .build
      .map { httpClient =>
        new ElasticSink[F](httpClient, config)
      }
}
