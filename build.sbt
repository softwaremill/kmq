import com.softwaremill.Publish.ossPublishSettings
import com.softwaremill.SbtSoftwareMillCommon.commonSmlBuildSettings
import sbt.Keys._
import sbt._

val scala2_13 = "2.13.10"

val kafkaVersion = "3.4.0"
val logbackVersion = "1.4.7"
val akkaVersion = "2.6.19"
val akkaStreamKafkaVersion = "2.1.1"
val scalaLoggingVersion = "3.9.5"
val scalaTestVersion = "3.2.15"
val catsEffectVersion = "3.4.9"
val fs2Version = "3.10.2"
val logs4CatsVersion = "2.6.0"
val fs2KafkaVersion = "2.6.0"

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
  addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"),
  ideSkipProject := (scalaVersion.value != scala2_13),
  mimaPreviousArtifacts := previousStableVersion.value.map(organization.value %% moduleName.value % _).toSet,
  mimaReportBinaryIssues := {
    if ((publish / skip).value) {} else mimaReportBinaryIssues.value
  }
)

lazy val kmq = (project in file("."))
  .settings(commonSettings)
  .settings(
    publishArtifact := false,
    name := "kmq",
    scalaVersion := scala2_13
  )
  .aggregate((core.projectRefs ++ exampleJava.projectRefs ++ exampleScala.projectRefs): _*)

lazy val core = (projectMatrix in file("core"))
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= List(
      "org.apache.kafka" % "kafka-clients" % kafkaVersion exclude ("org.scala-lang.modules", "scala-java8-compat"),
      "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
      "org.typelevel" %% "cats-effect" % catsEffectVersion,
      "co.fs2" %% "fs2-core" % fs2Version,
      "org.typelevel" %% "log4cats-core" % logs4CatsVersion,
      "org.typelevel" %% "log4cats-slf4j" % logs4CatsVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
      "org.scalatest" %% "scalatest-flatspec" % scalaTestVersion % Test,
      "io.github.embeddedkafka" %% "embedded-kafka" % kafkaVersion % Test exclude ("javax.jms", "jms"),
      "ch.qos.logback" % "logback-classic" % logbackVersion % Test,
      "com.github.fd4s" %% "fs2-kafka" % fs2KafkaVersion % Test
    )
  )
  .jvmPlatform(scalaVersions = Seq(scala2_13))

lazy val exampleJava = (projectMatrix in file("example-java"))
  .settings(commonSettings)
  .settings(
    publishArtifact := false,
    libraryDependencies ++= List(
      "org.apache.kafka" %% "kafka" % kafkaVersion,
      "io.github.embeddedkafka" %% "embedded-kafka" % kafkaVersion,
      "ch.qos.logback" % "logback-classic" % logbackVersion % Runtime
    )
  )
  .jvmPlatform(scalaVersions = Seq(scala2_13))
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
  .jvmPlatform(scalaVersions = Seq(scala2_13))
  .dependsOn(core)
