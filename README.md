# flink-kafka-scala-base

Base funcional mínima em Apache Flink + Kafka streaming com Scala.

**Este é um exemplo de framework principal para arquiteturas orientadas a eventos e processamento de big data.**

**Português (resumo):**
Demonstra arquitetura event-driven com Scala + Flink + Kafka. Ponto de partida funcional para pipelines de processamento em tempo real, ingestão de eventos de sistemas legados, enriquecimento com agentes IA/LLMs, sinks para Elasticsearch, DBs, outros tópicos Kafka. Escalável, com suporte a exactly-once (estenda com checkpoints).

**English:**

Minimal, functional Apache Flink + Kafka streaming base in Scala.

**This is a core framework example for event-driven architectures and big data processing.**

## Why this base?
- Directly demonstrates the architecture from real experience: "POC Python → full event-driven with Scala + Flink + Kafka adopted as platform standard" (Sicredi digital platform).
- Functional starting point for:
  - Real-time processing pipelines
  - Ingesting events from legacy systems
  - Enriching with AI agents / LLMs
  - Sinking to Elasticsearch, DBs, other Kafka topics
- Scalable, exactly-once capable (extend with checkpoints).

## Quick start (functional)

1. Start Kafka (easiest: use docker-compose or local Kafka).
   Example topics: `input-topic`, `output-topic`.

2. Run the job:
   ```bash
   sbt run
   ```

3. Produce messages to `input-topic` (e.g. via kafka-console-producer).
   See output in logs or sink.

The job reads from Kafka, processes (filter + enrich with timestamp + note), prints (or sink).

## Extend for production / our services

- **Legacy integration**: In the map/flatMap, call legacy Java/Play APIs, query old DBs, apply business rules from old systems.
- **AI Agents**: Call external agents (see whatsapp-grok-bot pattern) for smart routing, summarization, decisioning on events.
- **Elasticsearch**: Add sink for search/observability (high-volume platforms).
- **Full stack**: Combine with Play/Quarkus APIs that produce to Kafka, or Akka for other parts.
- **Docker / deploy**: Add Dockerfile + Flink on Kubernetes.

See Code Solutions site for the full context and other base projects:
https://ivamartins.github.io/code-solutions-site/

This project + the others (Play, AI agents, etc.) form a living set of functional frameworks proving the backend engineering, legacy modernization, and AI automation services.

## Next gaps this helps close
- Event-driven (Kafka/Flink)
- Scalable backends
- Integration with legacy + modern (AI on top)

Clone, customize package/org, add your real sources/sinks/logic.

## Building for Production / Flink Cluster

```bash
sbt assembly
# Produces target/scala-2.12/flink-kafka-scala-base-assembly-0.1.0-SNAPSHOT.jar
```

Submit to Flink:
```bash
./bin/flink run -c com.codesolutions.flink.EventStreamJob target/scala-2.12/flink-kafka-scala-base-assembly-0.1.0-SNAPSHOT.jar
```

## Local Full Environment (Recommended for Demo)

```bash
docker-compose up -d
# Wait for Kafka to be ready, then in another terminal:
sbt run
```

Use `kafka-console-producer` (from Kafka container or local) to send events.

## Extending This Base (Portfolio / Real Projects)

This project is designed as a **starting template** you can fork and evolve:

- Add custom deserializers for legacy formats (COBOL copybooks, fixed-length, old XML from mainframes or old Play/JSF systems).
- Call AI agents from the stream (see sibling repo `whatsapp-grok-bot` for the agent pattern; invoke via HTTP or CLI for enrichment, classification, decisioning).
- Add windowed aggregations, state, side outputs.
- Multiple sinks: Elasticsearch (for search/observability on high-volume platforms), JDBC for legacy DB sync, another Kafka topic.
- Exactly-once semantics + savepoints.
- Integrate with Play Framework frontends (see `play-scala-base`) that publish events to Kafka.

## Real-World Mapping

This base directly supports claims in professional experience and services:
- Event-driven transformation at scale (Sicredi).
- Legacy + modern integration.
- AI agents on top of streaming data.
- High-volume, real-time pipelines.

See the complete portfolio: https://ivamartins.github.io/code-solutions-site/
Company page: https://www.linkedin.com/company/code-solutions-it/

Clone → customize package name → add your business logic → you have a production-grade starting point.

