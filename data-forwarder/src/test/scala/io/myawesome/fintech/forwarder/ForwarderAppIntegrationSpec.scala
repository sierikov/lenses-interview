package io.myawesome.fintech.forwarder

import cats.effect.*
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all.*
import com.dimafeng.testcontainers.SchemaRegistryContainer
import com.dimafeng.testcontainers.lifecycle.and
import com.dimafeng.testcontainers.scalatest.TestContainersForEach
import io.myawesome.fintech.forwarder.forward.ForwarderService
import io.myawesome.fintech.forwarder.sink.ElasticSink
import io.myawesome.fintech.forwarder.sink.ElasticSinkSpec.extractClickRecords
import io.myawesome.fintech.forwarder.sink.ElasticSinkSpec.searchIndex
import io.myawesome.fintech.forwarder.source.KafkaDataSource
import io.myawesome.fintech.forwarder.utils.containers.TestElasticContainer
import io.myawesome.fintech.forwarder.utils.containers.TestKafkaContainer
import io.myawesome.fintech.forwarder.utils.generator.RandomClickRecordGenerator
import io.myawesome.fintech.forwarder.utils.producer.KafkaClickRecordProducer
import org.http4s.ember.client.EmberClientBuilder
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.DurationInt

class ForwarderAppIntegrationSpec extends AsyncFreeSpec with Matchers with TestContainersForEach with AsyncIOSpec {
  given logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  override type Containers = TestKafkaContainer and SchemaRegistryContainer and TestElasticContainer
  override def startContainers(): Containers = {
    val elasticContainer = TestElasticContainer.Def().start()
    val kafkaContainer   = TestKafkaContainer.Def().start()
    val schemaRegistryContainer = SchemaRegistryContainer.Def(
      kafkaContainer.network,
      kafkaContainer.networkAliases.head,
    ).start()

    kafkaContainer and schemaRegistryContainer and elasticContainer
  }

  "Forwarder" - {
    "read data from Kafka and place in Elastic in order" in withContainers {
      case kafkaContainer and schemaContainer and elasticContainer =>
        val topic           = "test-clicks"
        val numberOfRecords = 4
        val sourceConfig = KafkaDataSource.Config(
          topic             = topic,
          schemaRegistryUrl = schemaContainer.schemaUrl,
          bootstrapServers  = kafkaContainer.bootstrapServers,
          kafkaGroupId      = "testing-kafka-group",
        )

        val sinkConfig = ElasticSink.Config(
          host      = elasticContainer.host,
          port      = elasticContainer.mappedPort(9200),
          indexName = topic,
        )

        val forwardConfig = ForwarderService.Config(
          batchSize    = 2,
          batchTimeout = 2.seconds,
        )

        val testResource = for {
          httpClient <- EmberClientBuilder.default[IO].withTimeout(30.seconds).build
          producer <-
            KafkaClickRecordProducer.make[IO](topic, kafkaContainer.bootstrapServers, schemaContainer.schemaUrl)
          sink     <- ElasticSink.make[IO](sinkConfig)
          source   <- KafkaDataSource.make[IO](sourceConfig)
          forwarder = ForwarderService.make[IO](forwardConfig)

          generator    <- RandomClickRecordGenerator.make[IO].pure[IO].toResource
          randomEvents <- generator.generate(numberOfRecords).toResource
          _            <- randomEvents.traverse_(producer.send).toResource
          _            <- IO.sleep(2.seconds).toResource
          forwardFiber <- forwarder.forward(source, sink).background
          _            <- IO.sleep(2.seconds).toResource
          json         <- searchIndex(httpClient, sinkConfig.host, sinkConfig.port, sinkConfig.indexName + "-*").toResource
          clickDocs    <- IO.fromEither(extractClickRecords(json)).toResource
        } yield (randomEvents, clickDocs)

        testResource.use { case (expected, result) =>
          IO {
            result should have size numberOfRecords
            result should contain theSameElementsInOrderAs expected.init
          }
        }
    }
  }
}
