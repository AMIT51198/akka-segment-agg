ThisBuild / scalaVersion := "2.13.14"
ThisBuild / organization := "com.example"
ThisBuild / version := "1.0.0"

lazy val akkaVersion       = "2.8.8"
lazy val parquetVersion    = "1.14.4"
lazy val hadoopVersion     = "3.4.1"
lazy val logbackVersion    = "1.4.14"

lazy val root = (project in file("."))
  .settings(
    name := "akka-segment-agg",

    // Main entry point
    Compile / mainClass := Some("com.example.segmentagg.Main"),

    // Fork for run and test
    Compile / run / fork := true,
    Test / fork := true,

    // JVM options
    Compile / run / javaOptions ++= Seq(
      "-Xmx4g",
      "-XX:+UseG1GC"
    ),

    Test / javaOptions ++= Seq(
      "-Xmx2g"
    ),

    // Dependencies
    libraryDependencies ++= Seq(
      // Akka
      "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,

      // Parquet & Hadoop
      "org.apache.parquet" % "parquet-avro" % parquetVersion,
      "org.apache.parquet" % "parquet-hadoop" % parquetVersion,
      "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
      "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion,

      // Logging
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
      "ch.qos.logback" % "logback-classic" % logbackVersion,

      // Tests
      "org.scalatest" %% "scalatest" % "3.2.18" % Test,
      "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test,
      "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test
    )
  )
