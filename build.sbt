ThisBuild / scalaVersion     := "3.3.5"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "Zircuit",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % "2.1.17",
      "dev.zio" %% "zio-streams" % "2.1.17",
      "dev.zio" %% "zio-metrics-connectors" % "2.3.1",
      "dev.zio" %% "zio-test" % "2.1.17" % Test,
      "nl.vroste" %% "rezilience" % "0.10.4" % Test,
      "dev.zio" %% "zio-prelude" % "1.0.0-RC40",
      "dev.zio" %% "zio-test" % "2.1.17" % Test,
      "dev.zio" %% "zio-test-sbt" % "2.1.17" % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
