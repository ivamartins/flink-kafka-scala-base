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

## Quick Start / Como rodar a aplicação (Demo local)

**Pré-requisitos:** Java + sbt. Para o ambiente completo: Docker (recomendado).

**Passo a passo mais simples (apenas o job, sem Kafka real):**
- O job atual lê de stdin por padrão em alguns modos, mas para demo completa:

1. Inicie o ambiente Kafka (mais fácil com docker-compose incluso):
   ```bash
   docker-compose up -d
   ```
   Aguarde o Kafka ficar pronto.

2. Rode o job:
   ```bash
   sbt run
   ```

3. Produza mensagens (em outro terminal):
   Use `kafka-console-producer` (do container ou local) para o tópico de entrada.
   Veja o enriquecimento nos logs.

O job lê eventos, filtra, enriquece com timestamp + metadados e imprime/sink.

**English:**

**Prerequisites:** Java + sbt. For full local environment: Docker (recommended).

**Step-by-step (full local demo):**

1. Start the Kafka environment (easiest with the included docker-compose):
   ```bash
   docker-compose up -d
   ```
   Wait for Kafka to be ready.

2. Run the job:
   ```bash
   sbt run
   ```

3. Produce messages (in another terminal):
   Use `kafka-console-producer` (inside the container or locally) to the input topic.
   See enrichment in the logs.

The job reads events from Kafka, filters + enriches with timestamp/metadata, and prints (or sinks).

See the "Local Full Environment" section below for more details.

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

## Running the tests

**Português:**

```bash
sbt test
```

Testes unitários básicos (ScalaTest) cobrem a lógica de enriquecimento extraída (`enrichEvent`). Não precisam de Kafka, Zookeeper ou Flink rodando — são unitários puros e rápidos.

**English:**

```bash
sbt test
```

Basic unit tests (ScalaTest) cover the extracted enrichment logic (`enrichEvent`). They do not require Kafka, Zookeeper or a running Flink cluster — pure and fast unit tests.

For full streaming demo with real topics you still need the docker-compose environment.

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

