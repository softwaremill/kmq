import com.softwaremill.Publish.ossPublishSettings
import com.softwaremill.SbtSoftwareMillCommon.commonSmlBuildSettings
import sbt.Keys._
import sbt._

name := "kmq"
organization := "com.softwaremill.kmq"
version := "0.3.0-SNAPSHOT"

val scala2_12 = "2.12.16"
val scala2_13 = "2.13.8"

val scala2Versions = Seq(scala2_12, scala2_13)
val examplesScalaVersions = List(scala2_12)

lazy val commonSettings = commonSmlBuildSettings ++ ossPublishSettings ++ Seq(
  Test / parallelExecution := false,
  scalacOptions ++= Seq("-unchecked", "-deprecation"),
  evictionErrorLevel := Level.Info,

  // Sonatype OSS deployment
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (version.value.trim.endsWith("SNAPSHOT"))
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
  },
  credentials += Credentials(Path.userHome / ".ivy2" / ".credentials"),
  publishMavenStyle := true,
  Test / publishArtifact := false,
  pomIncludeRepository := { _ => false },
  pomExtra :=
    <scm>
      <url>git@github.com:softwaremill/kmq.git</url>
      <connection>scm:git:git@github.com:softwaremill/kmq.git</connection>
    </scm>
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
      "org.apache.kafka" % "kafka-clients" % "2.7.2" exclude("org.scala-lang.modules", "scala-java8-compat"),
      "com.typesafe.akka" %% "akka-actor" % "2.6.19",
      "com.typesafe.akka" %% "akka-stream" % "2.6.19",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
      "org.scalatest" %% "scalatest" % "3.2.12" % Test,
      "org.scalatest" %% "scalatest-flatspec" % "3.2.12" % Test,
      "com.typesafe.akka" %% "akka-testkit" % "2.6.19" % Test,
      "com.typesafe.akka" %% "akka-stream-kafka" % "2.1.1" % Test,
      "io.github.embeddedkafka" %% "embedded-kafka" % "2.7.0" % Test exclude("javax.jms", "jms"),
      "ch.qos.logback" % "logback-classic" % "1.2.11" % Test
    )
  )
  .jvmPlatform(scalaVersions = scala2Versions)

lazy val exampleJava = (projectMatrix in file("example-java"))
  .settings(commonSettings)
  .settings(
    publishArtifact := false,
    libraryDependencies ++= List(
      "org.apache.kafka" %% "kafka" % "2.7.2",
      "io.github.embeddedkafka" %% "embedded-kafka" % "2.7.0",
      "ch.qos.logback" % "logback-classic" % "1.2.11" % Runtime
    )
  )
  .jvmPlatform(scalaVersions = examplesScalaVersions)
  .dependsOn(core)

lazy val exampleScala = (projectMatrix in file("example-scala"))
  .settings(commonSettings)
  .settings(
    publishArtifact := false,
    libraryDependencies ++= List(
      "com.typesafe.akka" %% "akka-stream-kafka" % "2.1.1",
      "ch.qos.logback" % "logback-classic" % "1.2.11" % Runtime
    )
  )
  .jvmPlatform(scalaVersions = examplesScalaVersions)
  .dependsOn(core)
