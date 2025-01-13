package io.myawesome.fintech.forwarder.sink

import cats.*
import cats.syntax.all.*
import cats.effect.*
import cats.effect.syntax.all.*
import cats.effect.testing.scalatest.AsyncIOSpec
import com.dimafeng.testcontainers.scalatest.TestContainerForEach
import io.circe.Decoder
import io.circe.Json
import io.myawesome.fintech.avro.ClickRecord
import io.myawesome.fintech.forwarder.Codecs.given
import io.myawesome.fintech.forwarder.sink.ElasticSinkSpec.*
import io.myawesome.fintech.forwarder.utils.containers.TestElasticContainer
import io.myawesome.fintech.forwarder.utils.generator.RandomClickRecordGenerator
import org.http4s.*
import org.http4s.circe.*
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.Instant
import scala.concurrent.duration.*

class ElasticSinkSpec extends AsyncFreeSpec with Matchers with AsyncIOSpec with TestContainerForEach {

  given logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  override val containerDef: TestElasticContainer.Def = TestElasticContainer.Def()

  "ElasticSink" - {
    "should index documents in Elasticsearch" in withContainers { esContainer =>
      val indexName = "test-clicks"
      val sinkConfig = ElasticSink.Config(
        host      = esContainer.host,
        port      = esContainer.mappedPort(9200),
        indexName = "test-clicks",
      )

      val generator       = RandomClickRecordGenerator.make[IO]
      val numberOfRecords = 5

      val testResource = for {
        httpClient   <- EmberClientBuilder.default[IO].withTimeout(30.seconds).build
        sink         <- ElasticSink.make[IO](sinkConfig)
        randomEvents <- generator.generate(numberOfRecords).toResource
      } yield (httpClient, sink, randomEvents)

      testResource.use { case (httpClient, sink, randomEvents) =>
        val now                  = Instant.parse("2025-01-01T01:01:01Z") // same index for all events
        val eventsWithTimestamps = randomEvents.map(r => (now, r))
        for {
          _         <- sink.sendBatch(eventsWithTimestamps)
          _         <- IO.sleep(2.seconds) // give ES a moment to index
          json      <- searchIndex(httpClient, sinkConfig.host, sinkConfig.port, ElasticSink.createIndex(indexName, now))
          totalDocs <- IO.fromEither(json.hcursor.downField("hits").downField("total").downField("value").as[Int])
          docs      <- IO.fromEither(extractClickRecords(json))
          _ <- IO {
            totalDocs should be >= numberOfRecords
            docs should contain theSameElementsAs randomEvents
          }
        } yield succeed
      }
    }
  }
}

object ElasticSinkSpec {
  def searchIndex(
    httpClient: Client[IO],
    host:       String,
    port:       Int,
    indexName:  String,
  ): IO[Json] = {
    val uri = Uri.unsafeFromString(s"http://$host:$port/$indexName/_search")
    httpClient.expect[Json](Request[IO](Method.GET, uri))
  }

  def extractClickRecords(json: Json): Either[Throwable, List[ClickRecord]] =
    json.hcursor
      .downField("hits")
      .downField("hits")
      .as[List[Json]]
      .flatMap { docsJsonArray =>
        docsJsonArray.traverse { docJson =>
          docJson.hcursor
            .downField("_source")
            .as[ClickRecord]
        }
      }
}
