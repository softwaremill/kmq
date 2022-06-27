import com.softwaremill.Publish.ossPublishSettings
import com.softwaremill.SbtSoftwareMillCommon.commonSmlBuildSettings
import sbt.Keys._
import sbt._

name := "kmq"

val scala2_11 = "2.11.12"
val scala2_12 = "2.12.16"
val scala2_13 = "2.13.8"
val scala3 = "3.1.2"

val scala2Versions = Seq(scala2_11, scala2_12, scala2_13)
val scala2And3Versions = scala2Versions ++ List(scala3)

lazy val commonSettings = commonSmlBuildSettings ++ ossPublishSettings ++ Seq(
  organization := "com.softwaremill.kmq",
  version := "0.2.3",
  scalaVersion := scala2_12,
  crossScalaVersions := scala2Versions,

  ideSkipProject := (scalaVersion.value == scala2_13) ||
    (scalaVersion.value == scala3) ||
    thisProjectRef.value.project.contains("Native") ||
    thisProjectRef.value.project.contains("JS"),
  // slow down for CI
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
      "org.apache.kafka" % "kafka-clients" % "2.1.0",
      "com.typesafe.akka" %% "akka-actor" % "2.5.19",
      "com.typesafe.akka" %% "akka-stream" % "2.5.19",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
      "org.scalatest" %% "scalatest" % "3.0.5" % "test",
      "com.typesafe.akka" %% "akka-testkit" % "2.5.19" % "test",
      "com.typesafe.akka" %% "akka-stream-kafka" % "0.22" % "test",
      "net.manub" %% "scalatest-embedded-kafka" % "2.0.0" % "test" exclude("javax.jms", "jms")
    )
  )
  .jvmPlatform(scalaVersions = scala2Versions)

lazy val exampleJava = (projectMatrix in file("example-java"))
  .settings(commonSettings)
  .settings(
    publishArtifact := false,
    libraryDependencies ++= List(
      "org.apache.kafka" %% "kafka" % "2.1.0",
      "net.manub" %% "scalatest-embedded-kafka" % "2.0.0"
    )
  )
  .dependsOn(core)

lazy val exampleScala = (projectMatrix in file("example-scala"))
  .settings(commonSettings)
  .settings(
    publishArtifact := false,
    libraryDependencies ++= List(
      "com.typesafe.akka" %% "akka-stream-kafka" % "0.22",
      "ch.qos.logback" % "logback-classic" % "1.2.3"
    )
  )
  .dependsOn(core)
