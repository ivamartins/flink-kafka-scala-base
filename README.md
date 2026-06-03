# flink-kafka-scala-base

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
