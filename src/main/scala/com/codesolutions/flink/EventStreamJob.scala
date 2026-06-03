package com.codesolutions.flink

import org.apache.flink.streaming.api.scala._

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
 * ## Quick Start (Functional - no Kafka needed for base demo)
 * 1. sbt run
 * 2. The job will emit sample events from a collection (simulating legacy events).
 *
 * For full Kafka version (recommended for portfolio demo):
 * - Uncomment the Kafka imports and code below.
 * - Add the dependency in build.sbt: "org.apache.flink" % "flink-connector-kafka" % "1.18.1"
 * - docker-compose up -d (from the docker-compose.yml)
 * - sbt run
 * - Produce to input-topic with kafka-console-producer.
 *
 * ## How to Extend (as a Base Framework)
 * - Add custom deserializers for legacy formats (XML, fixed-length, mainframe).
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
 *
 * PT: Base funcional para arquiteturas event-driven com Scala + Flink + Kafka.
 * Demonstra pipelines em tempo real, ingestão de legados, enriquecimento com IA.
 * Mapeia para experiência Sicredi (POC -> plataforma padrão com Flink+Kafka) e serviços de Code Solutions.
 * Estenda com desserializadores legados, agentes IA, sinks ES, etc. Clone e customize.
 */
object EventStreamJob {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // For production-like behavior
    env.enableCheckpointing(5000)

    // === BASE DEMO (compiles without extra deps): simple streaming from collection ===
    val stream: DataStream[String] = env.fromElements(
      """{"id":1,"type":"legacy-order","amount":1250.5}""",
      """{"id":2,"type":"legacy-payment","amount":89.99}""",
      """{"id":3,"type":"legacy-order","amount":450.0}"""
    )

    // Core processing pipeline - this is where you put legacy rules, AI calls, transformations
    val processed: DataStream[String] = stream
      .filter(_.trim.nonEmpty)
      .map(rawEvent => enrichEvent(rawEvent))

    // Output for demo (in prod replace with proper sink)
    processed.print()

    // === FULL KAFKA VERSION (uncomment and add the connector dep in build.sbt) ===
    /*
    import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
    import org.apache.flink.api.common.serialization.SimpleStringSchema
    import java.util.Properties

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

    val kafkaStream: DataStream[String] = env.addSource(consumer)

    val kafkaProcessed: DataStream[String] = kafkaStream
      .filter(_.trim.nonEmpty)
      .map { rawEvent =>
        val ts = System.currentTimeMillis()
        s"ENRICHED[$ts] from_legacy_event: $rawEvent | processed_by=flink-kafka-base | org=CodeSolutions"
      }

    kafkaProcessed.print()

    // val producerProps = new Properties()
    // producerProps.setProperty("bootstrap.servers", brokers)
    // val producer = new FlinkKafkaProducer[String]("output-topic", new SimpleStringSchema(), producerProps)
    // kafkaProcessed.addSink(producer)
    */

    env.execute("Flink Streaming Base - Code Solutions (Sicredi-style event platform example)")
  }

  /**
   * Basic enrichment logic extracted for unit testing.
   * In real use: parse, apply legacy business rules, call AI agents, etc.
   */
  def enrichEvent(rawEvent: String): String = {
    val ts = System.currentTimeMillis()
    s"ENRICHED[$ts] from_legacy_event: $rawEvent | processed_by=flink-kafka-base | org=CodeSolutions"
  }
}
