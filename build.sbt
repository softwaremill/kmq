import sbt._
import Keys._

name := "kmq"

lazy val commonSettings = Seq(
  organization := "com.softwaremill.kmq",
  version := "0.1",
  scalaVersion := "2.12.2",
  crossScalaVersions := List(scalaVersion.value, "2.11.11"),

  scalacOptions ++= Seq("-unchecked", "-deprecation"),

  // Sonatype OSS deployment
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (version.value.trim.endsWith("SNAPSHOT"))
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases"  at nexus + "service/local/staging/deploy/maven2")
  },
  credentials   += Credentials(Path.userHome / ".ivy2" / ".credentials"),
  publishMavenStyle := true,
  publishArtifact in Test := false,
  pomIncludeRepository := { _ => false },
  pomExtra :=
    <scm>
      <url>git@github.com:softwaremill/kmq.git</url>
      <connection>scm:git:git@github.com:softwaremill/kmq.git</connection>
    </scm>
      <developers>
        <developer>
          <id>adamw</id>
          <name>Adam Warski</name>
          <url>http://www.warski.org</url>
        </developer>
      </developers>,
  licenses := ("Apache2", new java.net.URL("http://www.apache.org/licenses/LICENSE-2.0.txt")) :: Nil,
  homepage := Some(new java.net.URL("https://www.softwaremill.com/open-source"))
)

lazy val kmq = (project in file("."))
  .settings(commonSettings)
  .settings(
    publishArtifact := false
  )
  .aggregate(core, exampleJava, exampleScala)

lazy val core = (project in file("core"))
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= List(
      "org.apache.kafka" % "kafka-clients" % "0.10.2.0",
      "com.typesafe.akka" %% "akka-actor" % "2.5.0",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
    )
  )

lazy val exampleJava = (project in file("example-java"))
  .settings(commonSettings)
  .settings(
    publishArtifact := false,
    libraryDependencies ++= List(
      "org.apache.kafka" %% "kafka" % "0.10.2.0",
      "net.manub" %% "scalatest-embedded-kafka" % "0.13.0"
    )
  ) dependsOn(core)

lazy val exampleScala = (project in file("example-scala"))
  .settings(commonSettings)
  .settings(
    publishArtifact := false,
    libraryDependencies ++= List(
      "com.typesafe.akka" %% "akka-stream-kafka" % "0.15",
      "ch.qos.logback" % "logback-classic" % "1.2.3"
    )
  ) dependsOn(core)
