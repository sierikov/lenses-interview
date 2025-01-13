package io.myawesome.fintech.forwarder.utils.containers

import com.dimafeng.testcontainers.{ContainerDef, SingleContainer}
import io.myawesome.fintech.forwarder.utils.containers.TestElasticContainer
import org.testcontainers.elasticsearch.ElasticsearchContainer as JavaElasticsearchContainer
import org.testcontainers.utility.DockerImageName

import java.net.InetSocketAddress

case class TestElasticContainer(
  dockerImageName: DockerImageName = DockerImageName.parse(TestElasticContainer.defaultDockerImageName),
) extends SingleContainer[JavaElasticsearchContainer] {

  override val container: JavaElasticsearchContainer = new JavaElasticsearchContainer(dockerImageName)
    .withEnv("xpack.security.enabled", "false")

  def httpHostAddress: String = container.getHttpHostAddress

  def tcpHost: InetSocketAddress = container.getTcpHost
}

object TestElasticContainer {

  val defaultImage           = "docker.elastic.co/elasticsearch/elasticsearch"
  val defaultTag             = "7.8.0"
  val defaultDockerImageName = s"$defaultImage:$defaultTag"

  case class Def(
    dockerImageName: DockerImageName = DockerImageName.parse(TestElasticContainer.defaultDockerImageName),
  ) extends ContainerDef {

    override type Container = TestElasticContainer

    override def createContainer(): TestElasticContainer =
      new TestElasticContainer(dockerImageName)
  }
}
