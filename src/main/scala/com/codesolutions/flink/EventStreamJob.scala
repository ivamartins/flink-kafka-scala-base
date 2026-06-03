package com.codesolutions.flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.common.serialization.SimpleStringSchema
import java.util.Properties

/**
 * Flink + Kafka Streaming Base
 *
 * Functional example of event-driven architecture (Scala + Flink + Kafka).
 * 
 * Ties directly to:
 * - Sicredi experience: "evolução completa para arquitetura orientada a eventos com Scala + Apache Flink + Kafka — adotada como padrão"
 * - Services: "Arquiteturas orientadas a eventos e big data (processamento em tempo real, pipelines)", "Backends escaláveis"
 *
 * How to run (base):
 * 1. Start Kafka (docker-compose or local)
 * 2. sbt run
 * 3. Produce to 'input-topic', consume from 'output-topic'
 *
 * Extend for:
 * - Real legacy event ingestion (from old systems)
 * - AI enrichment (call agents/LLMs in the map/flatMap)
 * - Elasticsearch sink for search/observability
 * - Exactly-once, state, checkpoints for production
 *
 * This is a minimal, functional starting framework. Clone, rename, evolve.
 */
object EventStreamJob {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "flink-consumer-group")

    val kafkaConsumer = new FlinkKafkaConsumer[String](
      "input-topic",
      new SimpleStringSchema(),
      properties
    )

    val stream: DataStream[String] = env.addSource(kafkaConsumer)

    // Example processing: enrich, filter, transform (extend here for legacy rules, AI calls, etc.)
    val processed: DataStream[String] = stream
      .filter(_.nonEmpty)
      .map(line => s"PROCESSED: $line | timestamp=${System.currentTimeMillis()} | source=legacy-event")

    processed.print()  // In prod: sink to Kafka, ES, DB, or trigger agents

    // To output topic (uncomment and configure producer)
    // processed.addSink(new FlinkKafkaProducer[String]("output-topic", new SimpleStringSchema(), properties))

    env.execute("Flink Kafka Event Stream Base - Code Solutions Example")
  }
}
