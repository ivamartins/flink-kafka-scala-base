name := "flink-kafka-scala-base"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.12.18"  // Flink 1.18+ compatible with Scala 2.12

val flinkVersion = "1.18.1"

libraryDependencies ++= Seq(
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
  "org.apache.flink" % "flink-clients" % flinkVersion % "provided",
  // Test
  "org.scalatest" %% "scalatest" % "3.2.17" % Test
  // Add for full Kafka example:
  // "org.apache.flink" % "flink-connector-kafka" % flinkVersion
)

// For fat JAR (Flink jobs are usually submitted as fat jars)
assembly / mainClass := Some("com.codesolutions.flink.EventStreamJob")
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
