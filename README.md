# flink-kafka-scala-base

> Part of the **Code Solutions Event-Driven & Streaming Toolkit** product line. Functional starting point for real-time pipelines with Apache Flink and Apache Kafka in Scala.

Minimal, functional Apache Flink + Kafka streaming base in Scala. Demonstrates the event-driven architecture pattern adopted at the **Sicredi Digital** platform (PFM crawler → full event-driven stack that became the platform standard).

## Why this base

- **Proven pattern**: same architecture from real experience — "Python POC → full event-driven with Scala + Flink + Kafka adopted as platform standard" at Sicredi.
- **Functional starting point** for:
  - Real-time processing pipelines
  - Event ingestion from legacy systems
  - Enrichment with AI agents / LLMs
  - Sinks to Elasticsearch, databases, other Kafka topics
- **Scalable** with exactly-once semantics (extend with checkpoints)

## Quick start

**Prerequisites:** Java + sbt + Docker (for Kafka + Flink).

```bash
# 1) Start Kafka (and optionally Flink)
docker compose up -d

# 2) Run the example
sbt run
```

It will start a streaming job that ingests events from Kafka, processes them, and writes to a sink topic.

## Run the tests

```bash
sbt test
```

## Extend for real use

- Add your own `SourceFunction` / `FlinkKafkaConsumer` for legacy data ingestion
- Wire to Akka Typed actors for stateful processing
- Add AI enrichment step (LangChain4j, MCP, RAG)
- Add exactly-once checkpoints
- Connect to Elasticsearch sink (see `elasticsearch-scala-base`)

## Tech stack

- Scala 2.12
- Apache Flink 1.x
- Apache Kafka 2.x
- sbt build tool

> **Português?** Veja [`README.pt-BR.md`](./README.pt-BR.md).

## See also

- **Related base**: [akka-scala-base](https://github.com/ivamartins/akka-scala-base), [elasticsearch-scala-base](https://github.com/ivamartins/elasticsearch-scala-base)
- **Product line**: [Event-Driven & Streaming Toolkit](https://ivamartins.github.io/code-solutions-site/#produtos)
- **Code Solutions on LinkedIn**: [linkedin.com/company/code-solutions-it](https://www.linkedin.com/company/code-solutions-it/)
- **All Code Solutions open source**: [github.com/ivamartins](https://github.com/ivamartins)

## License

MIT — see `LICENSE`.
