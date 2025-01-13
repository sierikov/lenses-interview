package io.myawesome.fintech.forwarder.utils.containers

import com.dimafeng.testcontainers.ContainerDef
import com.dimafeng.testcontainers.SingleContainer
import org.testcontainers.containers.KafkaContainer as JavaKafkaContainer
import org.testcontainers.containers.Network
import org.testcontainers.utility.DockerImageName

/** Kafka container definition with ability to pass Network.
  * @param network
  *   network in which container should start
  * @param dockerImageName
  *   docker image name of Kafka
  */
case class TestKafkaContainer(
  override val network: Network         = TestKafkaContainer.defaultNetwork,
  dockerImageName:      DockerImageName = TestKafkaContainer.defaultDockerImage,
) extends SingleContainer[JavaKafkaContainer] {

  override val container: JavaKafkaContainer = new JavaKafkaContainer(dockerImageName).withNetwork(network)

  def bootstrapServers: String = container.getBootstrapServers
}

object TestKafkaContainer {

  val defaultImage = "confluentinc/cp-kafka"
  val defaultTag   = "7.6.1"

  private val defaultDockerImage = DockerImageName.parse(s"confluentinc/cp-kafka:$defaultTag")
  private def defaultNetwork: Network = Network.newNetwork()

  case class Def(
    network:         Network         = defaultNetwork,
    dockerImageName: DockerImageName = defaultDockerImage,
  ) extends ContainerDef {

    override type Container = TestKafkaContainer

    override def createContainer(): TestKafkaContainer =
      new TestKafkaContainer(network, dockerImageName)
  }
}
