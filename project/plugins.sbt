val sbtSoftwareMillVersion = "2.0.12"
addSbtPlugin("com.softwaremill.sbt-softwaremill" % "sbt-softwaremill-common" % sbtSoftwareMillVersion)
addSbtPlugin("com.softwaremill.sbt-softwaremill" % "sbt-softwaremill-publish" % sbtSoftwareMillVersion)
addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "1.1.2")
addSbtPlugin("org.scalameta" % "sbt-mdoc" % "2.3.7")
addSbtPlugin("com.eed3si9n" % "sbt-projectmatrix" % "0.9.0")
addSbtPlugin("org.jetbrains.scala" % "sbt-ide-settings" % "1.1.1")
addSbtPlugin("io.spray" % "sbt-revolver" % "0.10.0")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.5")
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.11.0")
