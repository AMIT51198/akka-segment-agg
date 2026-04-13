ThisBuild / scalaVersion := "2.13.14"
ThisBuild / organization := "com.example"
ThisBuild / version := "0.1.0"

lazy val akkaVersion = "2.8.8"
lazy val arrowVersion = "17.0.0"
lazy val parquetVersion = "1.14.4"
lazy val hadoopVersion = "3.4.1"

lazy val root = (project in file("."))
  .settings(
    name := "akka-segment-agg",
    Compile / run / mainClass := Some("com.example.segmentagg.Main"),
    Compile / run / fork := true,
    Compile / run / javaOptions ++= Seq(
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "-Dorg.slf4j.simpleLogger.logFile=System.out",
      "-Dorg.slf4j.simpleLogger.log.org.apache.parquet.hadoop.InternalParquetRecordReader=warn",
      "-Dorg.slf4j.simpleLogger.log.org.apache.hadoop.io.compress.CodecPool=warn"
    ),
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
      "org.apache.arrow" % "arrow-vector" % arrowVersion,
      "org.apache.arrow" % "arrow-memory-netty" % arrowVersion,
      "org.apache.parquet" % "parquet-avro" % parquetVersion,
      "org.apache.parquet" % "parquet-hadoop" % parquetVersion,
      "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
      "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion,
      "org.slf4j" % "slf4j-simple" % "2.0.16"
    )
  )
