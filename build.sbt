import com.softwaremill.Publish.ossPublishSettings
import com.softwaremill.SbtSoftwareMillCommon.commonSmlBuildSettings
import sbt.Keys._
import sbt._

val scala2_12 = "2.12.16"
val scala2_13 = "2.13.8"

val kafkaVersion = "2.7.2"
val embeddedKafkaVersion = "2.8.1"
val logbackVersion = "1.2.11"
val akkaVersion = "2.6.19"
val akkaStreamKafkaVersion = "2.1.1"
val scalaLoggingVersion = "3.9.5"
val scalaTestVersion = "3.2.12"

// slow down Tests for CI
parallelExecution in Global := false
concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)
// disable mima checks globally
mimaPreviousArtifacts in Global := Set.empty

lazy val commonSettings = commonSmlBuildSettings ++ ossPublishSettings ++ Seq(
  organization := "com.softwaremill.kmq",
  mimaPreviousArtifacts := Set.empty,
  versionScheme := Some("semver-spec"),
  scalacOptions ++= Seq("-unchecked", "-deprecation"),
  evictionErrorLevel := Level.Info,
  ideSkipProject := (scalaVersion.value != scala2_13),
  mimaPreviousArtifacts := previousStableVersion.value.map(organization.value %% moduleName.value % _).toSet,
  mimaReportBinaryIssues := {
    if ((publish / skip).value) {} else mimaReportBinaryIssues.value
  }
)

lazy val kmq = (projectMatrix in file("."))
  .settings(commonSettings)
  .settings(
    crossScalaVersions := Nil,
    publishArtifact := false
  )
  .aggregate(core, exampleJava, exampleScala)

lazy val core = (projectMatrix in file("core"))
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= List(
      "org.apache.kafka" % "kafka-clients" % kafkaVersion exclude ("org.scala-lang.modules", "scala-java8-compat"),
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
      "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
      "org.scalatest" %% "scalatest-flatspec" % scalaTestVersion % Test,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
      "com.typesafe.akka" %% "akka-stream-kafka" % akkaStreamKafkaVersion % Test,
      "io.github.embeddedkafka" %% "embedded-kafka" % embeddedKafkaVersion % Test exclude ("javax.jms", "jms"),
      "ch.qos.logback" % "logback-classic" % logbackVersion % Test
    )
  )
  .jvmPlatform(scalaVersions = Seq(scala2_12, scala2_13))

lazy val exampleJava = (projectMatrix in file("example-java"))
  .settings(commonSettings)
  .settings(
    publishArtifact := false,
    libraryDependencies ++= List(
      "org.apache.kafka" %% "kafka" % kafkaVersion,
      "io.github.embeddedkafka" %% "embedded-kafka" % embeddedKafkaVersion,
      "ch.qos.logback" % "logback-classic" % logbackVersion % Runtime
    )
  )
  .jvmPlatform(scalaVersions = Seq(scala2_12))
  .dependsOn(core)

lazy val exampleScala = (projectMatrix in file("example-scala"))
  .settings(commonSettings)
  .settings(
    publishArtifact := false,
    libraryDependencies ++= List(
      "com.typesafe.akka" %% "akka-stream-kafka" % akkaStreamKafkaVersion,
      "ch.qos.logback" % "logback-classic" % logbackVersion % Runtime
    )
  )
  .jvmPlatform(scalaVersions = Seq(scala2_12))
  .dependsOn(core)
