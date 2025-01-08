val http4sVersion = "0.23.30"

lazy val root = (project in file("."))
  .aggregate(dataGenerator, common, dataForwarder)
  .settings(
    name := "myawesome-fintech",
    ThisBuild / scalaVersion := "3.6.2",
    ThisBuild / version := "1.0.0"
  )

lazy val common = (project in file("common"))
  .settings(
    name := "common",
    libraryDependencies ++= Seq(
      "org.apache.avro" % "avro" % "1.12.0",
      "com.julianpeeters" %% "avrohugger-core" % "2.10.0",
      "com.github.fd4s" %% "vulcan" % "1.11.1",
      "com.github.fd4s" %% "vulcan-generic" % "1.11.1",
      "org.scalatest" %% "scalatest" % "3.2.19" % Test,
    ),
    Compile / sourceGenerators += (Compile / avroScalaGenerate).taskValue,
    Test / sourceGenerators += (Test / avroScalaGenerate).taskValue
  )

lazy val dataGenerator = (project in file("data-generator"))
  .settings(
    name := "data-generator",
    libraryDependencies ++= Seq(
      "com.github.fd4s" %% "vulcan" % "1.11.1",
      "com.github.fd4s" %% "vulcan-generic" % "1.11.1",
      "org.http4s" %% "http4s-core"         % http4sVersion,
      "org.http4s" %% "http4s-client"       % http4sVersion,
      "com.github.fd4s" %% "fs2-kafka" % "3.6.0",
      "org.typelevel" %% "cats-effect" % "3.5.7",
      "org.scalatest" %% "scalatest" % "3.2.19" % Test,
    )
  )
  .dependsOn(common)

lazy val dataForwarder = (project in file("data-forwarder"))
  .settings(
    name := "data-forwarder",
    libraryDependencies ++= Seq()
  )
  .dependsOn(common)