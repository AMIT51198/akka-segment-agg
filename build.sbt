ThisBuild / scalaVersion := "2.13.14"
ThisBuild / organization := "com.example"
ThisBuild / version := "1.0.0"

lazy val akkaVersion    = "2.8.8"
lazy val parquetVersion = "1.14.4"
lazy val hadoopVersion  = "3.4.1"

lazy val root = (project in file("."))
  .settings(
    name := "akka-segment-agg",
    Compile / mainClass := Some("com.example.segmentagg.Main"),
    Compile / run / fork := true,
    Test / fork := true,
    Compile / run / javaOptions ++= Seq(
      "-Xmx4g", "-XX:+UseG1GC",
      "-Dorg.slf4j.simpleLogger.log.org.apache.hadoop=warn",
      "-Dorg.slf4j.simpleLogger.log.org.apache.parquet=warn",
      "-Dorg.slf4j.simpleLogger.log.akka=warn"
    ),
    Test / javaOptions ++= Seq("-Xmx2g"),
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
      "com.typesafe.akka" %% "akka-stream"      % akkaVersion,
      "org.apache.parquet" % "parquet-hadoop"    % parquetVersion,
      "org.apache.hadoop"  % "hadoop-common"     % hadoopVersion,
      "org.apache.hadoop"  % "hadoop-mapreduce-client-core" % hadoopVersion,
      "org.slf4j"          % "slf4j-simple"      % "2.0.16",
      "org.scalatest"     %% "scalatest"         % "3.2.18" % Test,
      "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test
    )
  )
