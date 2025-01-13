import com.typesafe.sbt.packager.docker.DockerPlugin
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport.*

lazy val root = (project in file("."))
  .aggregate(dataForwarder)
  .settings(
    name                     := "myawesome-fintech",
    ThisBuild / scalaVersion := "3.6.2",
    ThisBuild / version      := "1.0.0",
    ThisBuild / resolvers ++= Seq(
      "confluent" at "https://packages.confluent.io/maven/",
    ),
  )

lazy val dataForwarder = (project in file("data-forwarder"))
  .enablePlugins(DockerPlugin)
  .enablePlugins(JavaAppPackaging)
  .settings(
    name := "data-forwarder",
    libraryDependencies ++= Seq(
      "com.github.pureconfig" %% "pureconfig-core"                    % "0.17.8",
      "org.apache.avro"        % "avro"                               % "1.12.0",
      "com.julianpeeters"     %% "avrohugger-core"                    % "2.11.0",
      "com.github.fd4s"       %% "vulcan"                             % "1.11.1",
      "com.github.fd4s"       %% "vulcan-generic"                     % "1.11.1",
      "org.typelevel"         %% "log4cats-slf4j"                     % "2.7.0",
      "ch.qos.logback"         % "logback-classic"                    % "1.5.16",
      "com.github.fd4s"       %% "fs2-kafka"                          % "3.6.0",
      "com.github.fd4s"       %% "fs2-kafka-vulcan"                   % "3.6.0",
      "org.typelevel"         %% "cats-effect"                        % "3.5.7",
      "org.http4s"            %% "http4s-ember-client"                % "0.23.30",
      "org.http4s"            %% "http4s-circe"                       % "0.23.30",
      "io.circe"              %% "circe-core"                         % "0.14.10",
      "io.circe"              %% "circe-generic"                      % "0.14.10",
      "io.circe"              %% "circe-parser"                       % "0.14.10",
      "org.scalatest"         %% "scalatest"                          % "3.2.19" % Test,
      "org.scalacheck"        %% "scalacheck"                         % "1.18.1" % Test,
      "org.typelevel"         %% "cats-effect-testing-scalatest"      % "1.6.0"  % Test,
      "com.dimafeng"          %% "testcontainers-scala-scalatest"     % "0.41.5" % Test,
      "com.dimafeng"          %% "testcontainers-scala-kafka"         % "0.41.5" % Test,
      "com.dimafeng"          %% "testcontainers-scala-elasticsearch" % "0.41.5" % Test,
    ),
    Compile / sourceGenerators += (Compile / avroScalaGenerate).taskValue,
    Compile / resourceDirectories += baseDirectory.value / "src" / "main" / "avro",
    Test / sourceGenerators += (Test / avroScalaGenerate).taskValue,
    Test / fork     := true,
    dockerBaseImage := "openjdk:21-slim",
    dockerLabels    := Map("version" -> version.value),
  )
