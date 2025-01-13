package io.myawesome.fintech.forwarder.source

import cats.effect.*
import cats.syntax.all.*
import cats.effect.testing.scalatest.AsyncIOSpec
import com.dimafeng.testcontainers.SchemaRegistryContainer
import com.dimafeng.testcontainers.lifecycle.and
import com.dimafeng.testcontainers.scalatest.TestContainersForEach
import io.myawesome.fintech.forwarder.utils.producer.KafkaClickRecordProducer
import io.myawesome.fintech.forwarder.utils.containers.TestKafkaContainer
import io.myawesome.fintech.forwarder.utils.generator.RandomClickRecordGenerator
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.duration.DurationInt

class KafkaDataSourceSpec extends AsyncFreeSpec with Matchers with TestContainersForEach with AsyncIOSpec {

  override type Containers = TestKafkaContainer and SchemaRegistryContainer
  override def startContainers(): Containers = {
    val kafkaContainer = TestKafkaContainer.Def().start()
    val schemaRegistryContainer = SchemaRegistryContainer.Def(
      kafkaContainer.network,
      kafkaContainer.networkAliases.head,
    ).start()

    kafkaContainer and schemaRegistryContainer
  }

  "Kafka Data Source" - {
    "read data from Kafka in order" in withContainers { case kafkaContainer and schemaContainer =>
      val topic           = "test-clicks"
      val numberOfRecords = 3
      val config = KafkaDataSource.Config(
        topic             = topic,
        schemaRegistryUrl = schemaContainer.schemaUrl,
        bootstrapServers  = kafkaContainer.bootstrapServers,
        kafkaGroupId      = "testing-kafka-group",
      )

      val setup = for {
        producer <- KafkaClickRecordProducer.make[IO](topic, kafkaContainer.bootstrapServers, schemaContainer.schemaUrl)
        source   <- KafkaDataSource.make[IO](config)
        generator = RandomClickRecordGenerator.make[IO]
      } yield (source, producer, generator)

      val testResource = for {
        (source, producer, generator) <- setup
        recordsToSend                 <- generator.generate(numberOfRecords).toResource
        _                             <- recordsToSend.traverse_(producer.send).toResource
        _                             <- IO.sleep(3.seconds).toResource
        consumed <- source
          .consume
          .parJoinUnbounded
          .take(numberOfRecords.toLong)
          .compile
          .toList
          .toResource
      } yield (recordsToSend, consumed.map(_.value))

      testResource.use { (expected, result) =>
        IO {
          result should have size numberOfRecords
          result should contain theSameElementsInOrderAs expected.init // we lose 1st message on topic creation
        }
      }
    }
  }
}
