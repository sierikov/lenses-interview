package io.myawesome.fintech.forwarder.sink

import cats.effect.*
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.*
import cats.syntax.all.*
import io.circe.parser.*
import io.myawesome.fintech.avro.ClickRecord
import io.myawesome.fintech.forwarder.Codecs.given
import io.myawesome.fintech.forwarder.sink.ElasticSink.Config
import io.myawesome.fintech.forwarder.utils.generator.ClickRecordGenerator
import io.myawesome.fintech.forwarder.utils.generator.RandomClickRecordGenerator
import io.myawesome.fintech.forwarder.utils.mock.HttpClientMock
import org.http4s.*
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.Instant
import scala.concurrent.duration.*

class ElasticSinkLogicSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  given logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  private val sinkConfig = Config(
    host      = "localhost",
    port      = 9200,
    indexName = "my-index",
  )

  // Should map into 2 different index times 10:00 and 10:15
  private val instants = List(
    Instant.parse("2025-01-13T10:12:00Z"),
    Instant.parse("2025-01-13T10:14:59Z"),
    Instant.parse("2025-01-13T10:16:00Z"),
    Instant.parse("2025-01-13T10:19:59Z"),
  )

  private val expected = List(
    s"${sinkConfig.indexName}-2025-01-13-10-00",
    s"${sinkConfig.indexName}-2025-01-13-10-00",
    s"${sinkConfig.indexName}-2025-01-13-10-15",
    s"${sinkConfig.indexName}-2025-01-13-10-15",
  )

  val successResponse: Response[IO] = Response(Status.Ok).withEntity("Bulk ok")

  val runImpl: Request[IO] => Resource[IO, Response[IO]] =
    _ => Resource.pure[IO, Response[IO]](successResponse)

  private def createDeps(
    runImpl: Request[IO] => Resource[IO, Response[IO]],
  ): IO[(ElasticSink[IO], HttpClientMock[IO], ClickRecordGenerator[IO])] = for {
    ref      <- Ref.of[IO, Vector[Request[IO]]](Vector.empty)
    mock      = HttpClientMock(ref, runImpl)
    sink      = new ElasticSink[IO](mock, sinkConfig)
    generator = RandomClickRecordGenerator.make[IO]
  } yield (sink, mock, generator)

  "ElasticSink" - {
    "index" - {
      "should be created from instant correctly" in {
        val result   = ElasticSink.createIndex(sinkConfig.indexName, Instant.parse("2025-01-13T10:14:59Z"))
        val result2  = ElasticSink.createIndex(sinkConfig.indexName, Instant.parse("2025-01-13T10:12:00Z"))
        val expected = s"${sinkConfig.indexName}-2025-01-13-10-00"
        result shouldBe expected
        result2 shouldBe expected
      }
    }
    "sendBatch" - {
      "should group events by their 15-minute index slot and make exactly 1 request per index" in {
        val testIO = for {
          tuple                    <- createDeps(runImpl)
          (sink, client, generator) = tuple
          randomEvents             <- generator.generate(instants.length)
          batch                     = instants.zip(randomEvents)
          _                        <- sink.sendBatch(batch)
          _                        <- IO.sleep(2.seconds)
          calls                    <- client.runCalls.get
          body                     <- calls.traverse(_.bodyText.compile.string)
          lines                     = body.map(_.split("\n").filter(_.nonEmpty).toList).reduce(_ ++ _)
          _                        <- logger.info(s"Lines: $lines")
        } yield {
          // Format:
          // {"index":{"_index":"my-index-2025-01-13-10-00"}}
          // {"field1":"c1","field2":"p1", ... }
          // {"index":{"_index":"my-index-2025-01-13-10-00"}}
          // {"field1":"c2","field2":"p2", ... }
          val indices = lines.flatMap { line =>
            parse(line).toOption.flatMap(_.hcursor.downField("index").downField("_index").as[String].toOption)
          }

          (calls.size, indices)
        }

        testIO.flatMap { case (callsAmount: Int, indices: List[String]) =>
          IO {
            callsAmount shouldBe 2
            indices should contain theSameElementsInOrderAs expected
          }
        }
      }

      "should produce valid NDJSON (action line + record line) in correct order" in {

        val testIO = for {
          (sink, client, generator) <- createDeps(runImpl)
          randomEvents              <- generator.generate(instants.length)
          batch                      = instants.zip(randomEvents)
          _                         <- sink.sendBatch(batch)
          calls                     <- client.runCalls.get
          body                      <- calls.traverse(_.bodyText.compile.string)
          lines                      = body.map(_.split("\n").filter(_.nonEmpty).toList).reduce(_ ++ _)
        } yield (calls.size, lines, randomEvents)

        testIO.flatMap { (callsAmount, lines, generatedEvents) =>
          IO {
            callsAmount shouldBe 2
            lines.grouped(2).zipWithIndex.foreach {
              case (Seq(action, record), idx) =>
                val parsedIndex = parse(action).toOption
                  .flatMap(_.hcursor.downField("index").downField("_index").as[String].toOption)
                parsedIndex shouldBe Some(expected(idx))

                val parsedRecord = parse(record).toOption
                  .flatMap(_.as[ClickRecord].toOption)
                parsedRecord shouldBe Some(generatedEvents(idx))

              case _ =>
                fail("Unexpected NDJSON structure: action line and record line pair expected")
            }
          }
        }

      }

      "should handle an empty list of events by doing nothing (no requests)" in {
        val events = List.empty[(Instant, ClickRecord)]

        val runImpl: Request[IO] => Resource[IO, Response[IO]] =
          _ => Resource.pure[IO, Response[IO]](Response(Status.Ok))

        val testIO = for {
          (sink, client, _) <- createDeps(runImpl)
          _                 <- sink.sendBatch(events)
          calls             <- client.runCalls.get

        } yield calls.size

        testIO.flatMap { callsAmount =>
          IO {
            callsAmount shouldBe 0
          }
        }
      }
    }
  }
}
