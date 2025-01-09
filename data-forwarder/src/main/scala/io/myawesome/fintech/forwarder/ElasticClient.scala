package io.myawesome.fintech.forwarder

import cats.effect._
import org.http4s.client._
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.{Method, Request, Uri}
import org.http4s.circe._
import io.circe.syntax._

import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import scala.concurrent.duration._

import io.myawesome.fintech.common.ClickRecord
import io.myawesome.fintech.forwarder.Codecs.given

trait ElasticClient[F[_]] {
  def sendEvent(event: ClickRecord): F[Unit]
}

object ElasticClient {

  /** Build a time-based index name, e.g. "my-events-2025-01-09-14-00"
   * (rounding to the nearest 15-min block).
   */
  private def indexName: String = {
    val now         = Instant.now().atOffset(ZoneOffset.UTC)
    val minutesSlot = (now.getMinute / 15) * 15 // e.g. 0, 15, 30, 45
    val roundedTime = now.withMinute(minutesSlot).withSecond(0).withNano(0)
    val formatter   = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm")
    s"my-events-${formatter.format(roundedTime)}"
  }

  def make[F[_]: Async](esHost: String, esPort: Int): Resource[F, ElasticClient[F]] = {
    EmberClientBuilder.default[F]
      .withTimeout(30.seconds)
      .build
      .map(client =>
        (event: ClickRecord) => {
          val idx = indexName
          val uri = Uri.unsafeFromString(s"http://$esHost:$esPort/$idx/_doc")
          val body = event.asJson

          val req = Request[F](
            method = Method.POST,
            uri = uri
          ).withEntity(body)

          client.run(req).use { resp =>
            if (resp.status.isSuccess)
              Async[F].unit
            else
              Async[F].raiseError(
                new RuntimeException(
                  s"Failed to index document. Status: ${resp.status}"
                )
              )
          }
        }
      )
  }
}

