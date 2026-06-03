package com.codesolutions.flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import java.util.Properties

/**
 * Flink + Kafka Streaming Base - Functional Portfolio Example
 *
 * This is a clean, runnable base project demonstrating real-time event-driven architecture
 * using Scala + Apache Flink + Kafka.
 *
 * Directly maps to professional experience:
 * - Sicredi (2014–2021): Started with Python POC → led full evolution to Scala + Flink + Kafka
 *   event-driven platform adopted as standard for the entire digital platform.
 * - Services offered via Code Solutions: event-driven architectures, big data pipelines,
 *   legacy system integration, scalable backends, AI agent enrichment on streams.
 *
 * ## Quick Start (Functional)
 * 1. docker-compose up -d   # Starts Kafka + Zookeeper (see docker-compose.yml)
 * 2. sbt run
 * 3. In another terminal: produce test events
 *    kafka-console-producer --broker-list localhost:9092 --topic input-topic
 *    (type messages like: {"orderId":123,"amount":99.9,"type":"legacy"})
 * 4. See enriched output in Flink logs.
 *
 * To produce to output-topic, uncomment the sink and configure a producer.
 *
 * ## How to Extend (as a Base Framework)
 * - Ingest from legacy systems: Replace SimpleStringSchema with custom deserializer for old formats (XML, fixed-length, mainframe).
 * - AI/LLM enrichment: In the .map, call external agents (see sibling whatsapp-grok-bot project) or REST APIs for smart routing, anomaly detection, data enrichment.
 * - Add state, windows, exactly-once: Enable checkpoints, use keyed state.
 * - Sinks: Add Elasticsearch, JDBC, another Kafka topic, or trigger webhooks/agents.
 * - Production: Build fat jar with `sbt assembly`, submit to Flink cluster.
 * - Combine with other bases: Feed from Play Framework APIs (see play-scala-base), or Scala Akka services.
 *
 * This project is intentionally minimal yet complete — clone, customize the package, add your domain logic, and use as proof for modernization or new event-driven platforms.
 *
 * See full portfolio and services: https://ivamartins.github.io/code-solutions-site/
 * Company LinkedIn: https://www.linkedin.com/company/code-solutions-it/
 */
object EventStreamJob {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // For production-like behavior
    env.enableCheckpointing(5000)

    val brokers = sys.env.getOrElse("BOOTSTRAP_SERVERS", "localhost:9092")

    val consumerProps = new Properties()
    consumerProps.setProperty("bootstrap.servers", brokers)
    consumerProps.setProperty("group.id", "flink-consumer-group")
    consumerProps.setProperty("auto.offset.reset", "earliest")

    val consumer = new FlinkKafkaConsumer[String](
      "input-topic",
      new SimpleStringSchema(),
      consumerProps
    )

    val stream: DataStream[String] = env.addSource(consumer)

    // Core processing pipeline - this is where you put legacy rules, AI calls, transformations
    val processed: DataStream[String] = stream
      .filter(_.trim.nonEmpty)
      .map { rawEvent =>
        // Example enrichment (in real use: parse JSON, apply business rules from legacy,
        // call AI agent for classification, add metadata, etc.)
        val ts = System.currentTimeMillis()
        s"ENRICHED[$ts] from_legacy_event: $rawEvent | processed_by=flink-kafka-base | org=CodeSolutions"
      }

    // Output for demo (in prod replace with proper sink)
    processed.print()

    // Example: write to output topic (uncomment for full pipeline)
    // val producerProps = new Properties()
    // producerProps.setProperty("bootstrap.servers", brokers)
    // val producer = new FlinkKafkaProducer[String](
    //   "output-topic",
    //   new SimpleStringSchema(),
    //   producerProps
    // )
    // processed.addSink(producer)

    env.execute("Flink Kafka Event Stream Base - Code Solutions (Sicredi-style event platform example)")
  }
}
