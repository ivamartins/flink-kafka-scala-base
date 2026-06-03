name := "flink-kafka-scala-base"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.12.18"  // Flink 1.18 compatible

val flinkVersion = "1.18.1"

libraryDependencies ++= Seq(
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  "org.apache.flink" %% "flink-connector-kafka" % flinkVersion,
  "org.apache.flink" % "flink-clients" % flinkVersion
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
