# Data Forwarder

Welcome! This project is a Scala application that consumes messages from an [Apache Kafka] topic and indexes them
into [Elasticsearch], ensuring that messages from each partition retain their processing order. The messages are
stored in Elasticsearch indices that rotate every 15 minutes based on the Kafka message timestamp.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Quickstart](#quickstart)
- [Application Overview](#application-overview)
- [Logging and Configuration](#logs-and-observability)
- [Tests](#test)

## Prerequisites

Before you begin, make sure you have the following installed:

- **[Docker]** and **Docker Compose**: To run Kafka and Elasticsearch services locally.
- **Scala 3**: For building and running the application.
- **[sbt]**: The Scala build tool for managing project dependencies and tasks.

## Quickstart

To deploy as quickly as possible you can follow these steps. First, clone the repository.

```bash
git clone git@github.com:sierikov/lenses-interview.git
```

Then enter the directory of the project and build the docker image with:

```bash
sbt dataForwarder/docker:publishLocal
```

We can verify that the image is present with:

```bash
docker image list | grep data-forwarder
```

following docker-compose snippet:

```yml
services:
  data-forwarder:
    image: data-forwarder:1.0.0
    restart: unless-stopped
    environment:
      # Configuration for your Kafka
      - SOURCE_BOOTSTRAP_SERVERS=your.kafka.server:9092
      - SOURCE_SCHEMA_REGISTRY_URL=http://your.schema.registry:8081
      - SOURCE_TOPIC=my-clicks
      - SOURCE_KAFKA_GROUP_ID=kafka-elastic-forwarder-group

      # Configuration for your Elastic
      - SINK_HOST=your.elasticsearch.host
      - SINK_PORT=9200
      - SINK_INDEX_NAME=my-clicks

      # Forwarder configuration overrides
      - FORWARDER_BATCH_SIZE=10
      - FORWARDER_BATCH_TIMEOUT=10s
```

The meaning of each configuration variable you will find below in [Configuration](#Configuration).
If you have already running Kafka, SchemaRegistry and Elasticsearch running please modify the docker-compose accordingly
and run it with:

```
docker compose up -d
```

If you don't have running instances, please check out [this docker-compose file](./dev/docker-compose.yml).
It describes [Lenses Dev Box] (Kafka + SchemaRegistry + UI), Elasticsearch and [Kibana](https://www.elastic.co/kibana).

After application is started you should see logs, indicating that records are being transferred to Elasticsearch.

## Application Overview

This application performs will consume Kafka avro messages from a topic with `n` partitions.
The order within a Kafka topic is saved. The `timestamp` from Kafka message is used to create
an index for Elasticsearch. The index is rotated every `15 minutes` and has `{topic_name}_{yyyy-MM-dd-HH:mm}` format.

The application is horizontal-scalable, every new instance will operate on its own partition.
If the application works alone, it will every partition of the topic in independent Fiber.
If there are more forwarders than partitions, that the extra forwarder will wait for the chance to
replace another, providing some level of redundancy.

### Technologies Used

The project was build using [Typelevel] ecosystem and [cats-effect] effect system.

### Error handling

Important to note, that this application lacks of self-healing mechanism. However, there is a list what is
already implemented :

- **Kafka Connectivity Issues**: The application will retry connections with interval and log appropriate error
  messages.
- **Unexpected crash**: If the application dies itself, the records will be replayed from the last processing position.
- **Double write chance**: To reduce chances of double write, this position is not commited until
  Elasticsearch acknowledges new records.

Here is what can be done to achieve more robust service:

- **Mitigate Double Write**: this problem can be solved if the `ClickRecord` will introduce unique identifier (for
  example: `UUIDv7`), or construct this identifier with help of fields, that are always present in the record. In this
  case, the Elasticsearch could reject duplicates, which solves a little, but a chance of document double write.
- **Schema Registry Errors**: Handle cases where the schema is not accessible or incompatible.
- **Elasticsearch Failures**: Implement exponential retries and buffers for situations, when Elasticsearch cannot or
  fails to receive new documents. This also requires consumption management to prevent Out-Of-Memory errors.
- **Message Processing**: Implement anti-corruption layer, to catch/skip incompatible records in the topic (if there is
  any such chance).
- **Minor Issues**: Manage possible minor errors in with `F.raiseError` and `handleErrorWith`.

### Logs and Observability

The application uses a logging framework to provide insight into its operations. Log levels can be adjusted in the
configuration.
Currently, there is no observability mechanism, but logs. As possible useful improvement
is introduction of OpenTelemetry metrics (custom and JVM).

## Build

To build/run this service from sources please follow these steps. Clone the repository:

```bash
git clone git@github.com:sierikov/lenses-interview.git
```

Enter the repository folder and compile it with:

```bash
sbt compile
```

Then/Or you can run it with:

```bash
sbt dataForwarder/run
```

Now you should see, that application is connecting to Kafka and start to sending messages into
Elasticsearch.

## Test

### Automatic tests

To run all tests in the project just type:

```bash
sbt test
```

> Note: there are no separation between integration and unit test for this time,
> so first run maybe slow, due to loading docker-images.

### Manual tests

If you want to try the service manually. Please first start [this docker compose](./dev/docker-compose.yml) to bring
Kafka, SchemaRegistry, Elasticsearch and Kibana.

```bash
docker compose -f ./dev/docker-compose.yml up -d
```

The [default configuration](./data-forwarder/src/main/resources/application.conf)
is set by default to correct values for this docker-compose. Then
you can start the application how you prefer with [sbt](#build) or [docker](#quickstart).

Then you can use built in test `Manual` records producer to produce random `ClickRecords`.

```bash
sbt dataForwarder/run io.myawesome.fintech.forwarder.manual.Main
```

You can configure generator
in [corresponding
`ManualConfig.scala`](./data-forwarder/src/test/scala/io/myawesome/fintech/forwarder/manual/ManualConfig.scala).

You should see log messages when a `ClickRecord` was published to Kafka. In the web browser you can open
[localhost:3030](http://localhost:3030/) to explore SchemaRegistry and Kafka topics. To see coming to Elastic indexes
please use Kibana at [localhost:5601](http://localhost:5601/).

## Configuration

Below you'll find a table with description of each configuration value for this data forwarder.

> Note: Environment variable will always override the value in the `application.conf`

| **Configuration Key**               | **Environment Variable**     | **Description**                                                                                           | **Default Value**               |
|-------------------------------------|------------------------------|-----------------------------------------------------------------------------------------------------------|---------------------------------|
| `source-config.bootstrap-servers`   | `SOURCE_BOOTSTRAP_SERVERS`   | The Kafka bootstrap servers used to connect to the Kafka cluster.                                         | `localhost:9092`                |
| `source-config.schema-registry-url` | `SOURCE_SCHEMA_REGISTRY_URL` | URL of the schema registry used for serializing and deserializing Kafka messages.                         | `http://localhost:8081`         |
| `source-config.topic`               | `SOURCE_TOPIC`               | The Kafka topic to consume messages from.                                                                 | `my-clicks`                     |
| `source-config.kafka-group-id`      | `SOURCE_KAFKA_GROUP_ID`      | The consumer group ID for Kafka, used to identify the consumer group for message offset tracking.         | `kafka-elastic-forwarder-group` |
| `sink-config.host`                  | `SINK_HOST`                  | The host address of the sink (e.g., Elasticsearch).                                                       | `localhost`                     |
| `sink-config.port`                  | `SINK_PORT`                  | The port of the sink (e.g., Elasticsearch).                                                               | `9200`                          |
| `sink-config.index-name`            | `SINK_INDEX_NAME`            | The index name where the data will be stored in the sink.                                                 | `my-clicks`                     |
| `forwarder-config.batch-size`       | `FORWARDER_BATCH_SIZE`       | The number of messages to process in a single batch before sending them to the sink.                      | `10`                            |
| `forwarder-config.batch-timeout`    | `FORWARDER_BATCH_TIMEOUT`    | The maximum time to wait before sending a batch of messages to the sink, even if the batch is incomplete. | `10s`                           |

[Apache Kafka]: https://kafka.apache.org/

[Elasticsearch]: https://www.elastic.co/elasticsearch

[Docker]: https://www.docker.com/get-started/

[sbt]: https://www.scala-sbt.org/

[Lenses Dev Box]: https://hub.docker.com/r/lensesio/fast-data-dev

[Typelevel]: https://typelevel.org/

[cats-effect]: https://typelevel.org/cats-effect/