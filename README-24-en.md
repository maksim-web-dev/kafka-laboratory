# Branch 24 — Advanced Topics: Compression, AdminClient, Interceptors

## 1. What we learn in this branch

- **Compression** — gzip / lz4 / zstd / snappy: trade-off between CPU cost and message size.
- **AdminClient API** — programmatic topic and consumer group management without the CLI.
- **ProducerInterceptor** — `onSend()`, `onAcknowledgement()`: hooks in the producer pipeline.
- **ConsumerInterceptor** — `onConsume()`, `onCommit()`: hooks in the consumer pipeline.
- **Benchmark** — comparison of all compression types by throughput and latency.
- **Quotas** — `producer_byte_rate` / `consumer_byte_rate`: per-client rate limits.

---

## 2. Changes compared to the previous branch (branch23)

- **Removed** SASL/SSL and ACL — reverted to PLAINTEXT for simplicity.
- **Added** admin-service — a new microservice exposing AdminClient via REST API.
- **order-service** extended: compression config, ProducerInterceptor,
  ConsumerInterceptor, benchmark endpoint.
- **New endpoint** `/api/benchmark/compare?count=N` — compression comparison.
- **New endpoints** in admin-service: topics CRUD, cluster info, consumer groups.

---

## 3. Architecture

```
order-service (compression + interceptors)
  POST /api/orders ──[24.orders.created]──► notification (implicit)
  POST /api/benchmark/compare ──► comparison of compression types
  GET  /api/interceptor/stats ──► interceptor statistics

admin-service (AdminClient)
  GET  /api/admin/topics ──► list topics
  POST /api/admin/topics ──► create topic
  GET  /api/admin/cluster ──► cluster info
  ...
```

**Services and ports (from docker-compose-24.yml):**
- `kafka` — 9092 (KRaft single-node)
- `kafka-ui` — 8080 → http://localhost:8080
- `order-service-b24` — 8081 → http://localhost:8081
- `admin-service-b24` — 8089 → http://localhost:8089

---

## 4. Key Concepts

### Compression types

```
Type      Compression ratio  CPU          Latency    Recommendation
none      0%                 minimal      minimal    dev, small messages
gzip      60–70%             high         high       batch processing, archiving
snappy    40–50%             medium       medium     Google (legacy)
lz4       40–50%             low          low        production (recommended)
zstd      50–60%             medium       medium     production (best ratio, Kafka 2.1+)
```

```yaml
# application.yml
spring:
  kafka:
    producer:
      compression-type: lz4  # or: gzip, snappy, zstd, none
```

### Compression at the broker level

```
compression.type=producer (default):
  Broker stores in the producer's format → no recompression, efficient

compression.type=gzip (broker overrides):
  Broker recompresses regardless of what the producer used
  → double compression, not recommended

compression.type=uncompressed:
  Broker always decompresses → also not recommended
```

### AdminClient API

```kotlin
val adminClient = AdminClient.create(mapOf(
    AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers
))

// List topics
adminClient.listTopics().listings().get()

// Create a topic
adminClient.createTopics(listOf(NewTopic("my-topic", 3, 1))).all().get()

// Describe a topic (partitions, ISR, leader)
adminClient.describeTopics(listOf("my-topic")).allTopicNames().get()

// Change configuration
val resource = ConfigResource(ConfigResource.Type.TOPIC, "my-topic")
val op = AlterConfigOp(ConfigEntry("retention.ms", "86400000"), AlterConfigOp.OpType.SET)
adminClient.incrementalAlterConfigs(mapOf(resource to listOf(op))).all().get()

// Consumer groups
adminClient.listConsumerGroups().all().get()
adminClient.describeConsumerGroups(listOf("my-group")).all().get()
```

### ProducerInterceptor

```kotlin
class MetricsProducerInterceptor : ProducerInterceptor<String, Any> {
    private val sentCount = AtomicLong(0)
    private val ackCount = AtomicLong(0)
    private val errorCount = AtomicLong(0)

    // BEFORE serialization and sending — can modify the record
    override fun onSend(record: ProducerRecord<String, Any>): ProducerRecord<String, Any> {
        sentCount.incrementAndGet()
        return record  // MUST return record
    }

    // AFTER receiving an ACK from the broker (or on error)
    override fun onAcknowledgement(metadata: RecordMetadata?, exception: Exception?) {
        if (exception == null) ackCount.incrementAndGet()
        else errorCount.incrementAndGet()
    }

    override fun close() {}
    override fun configure(configs: Map<String, *>) {}
}
```

### HeaderEnrichmentInterceptor

```kotlin
class HeaderEnrichmentInterceptor : ProducerInterceptor<String, Any> {
    override fun onSend(record: ProducerRecord<String, Any>): ProducerRecord<String, Any> {
        record.headers()
            .add("x-service-name", "order-service".toByteArray())
            .add("x-sent-at", System.currentTimeMillis().toString().toByteArray())
            .add("x-trace-id", UUID.randomUUID().toString().toByteArray())
        return record
    }
}
```

**Configuring multiple interceptors:**
```yaml
spring:
  kafka:
    producer:
      properties:
        interceptor.classes: >
          com.kafkalab.order.interceptor.MetricsProducerInterceptor,
          com.kafkalab.order.interceptor.HeaderEnrichmentInterceptor
```

### ConsumerInterceptor

```kotlin
class TracingConsumerInterceptor : ConsumerInterceptor<String, Any> {
    // BEFORE the listener processes — can filter/enrich records
    override fun onConsume(records: ConsumerRecords<String, Any>): ConsumerRecords<String, Any> {
        for (record in records) {
            val sentAt = record.headers().lastHeader("x-sent-at")
                ?.let { String(it.value()) }
            log.info("[INTERCEPTOR] topic={} offset={} sentAt={}",
                record.topic(), record.offset(), sentAt)
        }
        return records  // can return a filtered subset
    }

    // AFTER offset commit
    override fun onCommit(offsets: Map<TopicPartition, OffsetAndMetadata>) {
        log.debug("[COMMIT] offsets={}", offsets)
    }
}
```

---

## 5. How to Run

```bash
docker compose -f docker-compose-24.yml up --build

# Kafka UI:      http://localhost:8080
# order-service: http://localhost:8081
# admin-service: http://localhost:8089
```

---

## 6. How to Test

### A. Compression Benchmark

```bash
# Compare all compression types (100 messages of ~1KB each)
curl -s -X POST "http://localhost:8081/api/benchmark/compare?count=100" | jq .
# {
#   "none":  {"throughputMsgPerSec": 5000, "avgLatencyMs": 0.2},
#   "lz4":   {"throughputMsgPerSec": 4500, "avgLatencyMs": 0.22},
#   "gzip":  {"throughputMsgPerSec": 1000, "avgLatencyMs": 1.0},
#   "zstd":  {"throughputMsgPerSec": 2000, "avgLatencyMs": 0.5},
#   "snappy":{"throughputMsgPerSec": 3500, "avgLatencyMs": 0.28}
# }
```

### B. AdminClient API

```bash
# List topics
curl http://localhost:8089/api/admin/topics | jq .

# Cluster info (brokers, controller)
curl http://localhost:8089/api/admin/cluster | jq .

# Create a topic
curl -X POST http://localhost:8089/api/admin/topics \
  -H "Content-Type: application/json" \
  -d '{"name":"my-new-topic","partitions":3,"replicationFactor":1}' | jq .

# Describe a topic
curl http://localhost:8089/api/admin/topics/24.orders.created | jq .

# Topic configuration (non-default values)
curl http://localhost:8089/api/admin/topics/24.orders.created/config | jq .

# Change retention
curl -X PATCH http://localhost:8089/api/admin/topics/24.orders.created/config \
  -H "Content-Type: application/json" \
  -d '{"key":"retention.ms","value":"86400000"}' | jq .

# Consumer groups
curl http://localhost:8089/api/admin/consumer-groups | jq .
curl http://localhost:8089/api/admin/consumer-groups/notification-service-group | jq .

# Delete a topic
curl -X DELETE http://localhost:8089/api/admin/topics/my-new-topic | jq .
```

### C. Interceptor Stats

```bash
# Send a few orders
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-1","itemCount":2}' | jq .

# View interceptor statistics
curl http://localhost:8081/api/interceptor/stats | jq .
# {"sent": 1, "acked": 1, "errors": 0}

# Kafka UI → topic 24.orders.created → Message → Headers
# x-service-name: order-service
# x-sent-at: 1234567890123
# x-trace-id: uuid-here
```

### D. Quotas (bonus)

```bash
# Limit a producer to 1 MB/s
docker exec kafka kafka-configs \
  --bootstrap-server localhost:9092 \
  --alter \
  --add-config 'producer_byte_rate=1048576' \
  --entity-type clients \
  --entity-name order-service

# Verify
docker exec kafka kafka-configs \
  --bootstrap-server localhost:9092 \
  --describe \
  --entity-type clients \
  --entity-name order-service
```

---

## 7. Deep Dive

### When does onSend() fire?

```
Producer pipeline:
  1. kafkaTemplate.send(record)
  2. ProducerInterceptor.onSend(record) ← BEFORE serialization!
      → can modify key, value, headers, topic
      → MUST return record (even if unchanged)
  3. Serialization (key/value serializer)
  4. Partitioner
  5. Buffer (RecordAccumulator)
  6. Sender thread → broker

  ProducerInterceptor.onAcknowledgement(metadata, exception) ← AFTER ACK
```

### Compression and batching

```
Compression happens at the batch level (not per individual record):
  linger.ms=20 → producer waits 20ms to accumulate a batch
  → larger batch → better compression ratio

For maximum compression efficiency:
  compression-type: lz4 (or zstd)
  linger.ms: 20–50ms
  batch.size: 65536 (64KB)
```

### AdminClient vs kafka-topics CLI

```
kafka-topics CLI:
  Shell script, exits after execution
  Suitable for: one-off operations, CI/CD pipelines

AdminClient API:
  Java/Kotlin code, long-lived lifecycle
  Suitable for: dynamic topic management at runtime,
               self-service platforms, operational dashboards,
               automated provisioning

AdminClient.close() is MANDATORY (closes TCP connections)
```

---

## 8. Project Structure

```
branch24_advanced_topics/
├── order-service/
│   ├── controller/
│   │   ├── OrderController.kt          ← POST /api/orders
│   │   ├── BenchmarkController.kt      ← POST /api/benchmark/compare?count=N
│   │   └── InterceptorController.kt    ← GET /api/interceptor/stats
│   ├── interceptor/
│   │   ├── MetricsProducerInterceptor.kt ← sentCount, ackCount, errorCount
│   │   ├── HeaderEnrichmentInterceptor.kt ← x-service-name, x-sent-at, x-trace-id
│   │   └── TracingConsumerInterceptor.kt  ← log traceId on consume
│   └── service/BenchmarkService.kt     ← comparison of 5 compression types
└── admin-service/
    ├── controller/AdminController.kt   ← all /api/admin/* endpoints
    └── service/AdminClientService.kt   ← AdminClient operations
```

---

## 9. What's Next

- You have completed all 24 branches of Kafka Laboratory!
- Suggested topics for self-study:
  - Kafka MirrorMaker 2 (cross-cluster replication)
  - Tiered Storage (S3-backed log)
  - Kafka on Kubernetes (Strimzi operator)
  - Performance tuning (JVM, GC, OS tuning)

---

## 10. Lecture Slides

### Slide 1 — Compression Trade-offs

```
          Size      CPU      Latency
none:    ████████  ░░       ░░░░░░
lz4:     ████░░░   ░░░      ░░░░░░░
snappy:  ████░░░   ░░░░     ░░░░░░░░
zstd:    ███░░░░   ░░░░░    ░░░░░░░░░
gzip:    ██░░░░░   ░░░░░░   ░░░░░░░░░░

Production recommendation: lz4 or zstd
```

### Slide 2 — Interceptor Pipeline

```
send(record)
    │
    ▼
[onSend interceptor 1]  ← can modify record
    │
    ▼
[onSend interceptor 2]  ← can modify record
    │
    ▼
[Serializer]
    │
    ▼
[Partitioner]
    │
    ▼
[Buffer → Broker]
    │
    ▼
[onAcknowledgement]     ← AFTER receiving ACK
```

### Slide 3 — AdminClient use cases

```
kafka-topics CLI:            AdminClient API:
  One-off                      Runtime (in code)
  ─────────────                ─────────────────────
  CI/CD pipeline               Self-service platform
  Manual ops                   Auto-provisioning
  Dev environment              Multi-tenant management
                               Topic health monitoring
```

---

## 11. Demo Script

```bash
#!/bin/bash
echo "=== Branch 24: Advanced Topics Demo ==="

docker compose -f docker-compose-24.yml up --build -d
sleep 60

echo ""
echo "=== A. Compression Benchmark ==="
curl -s -X POST "http://localhost:8081/api/benchmark/compare?count=50" | jq .

echo ""
echo "=== B. AdminClient: list topics ==="
curl -s http://localhost:8089/api/admin/topics | jq '.[].name'

echo ""
echo "=== B. AdminClient: cluster info ==="
curl -s http://localhost:8089/api/admin/cluster | jq .

echo ""
echo "=== B. AdminClient: create topic ==="
curl -s -X POST http://localhost:8089/api/admin/topics \
  -H "Content-Type: application/json" \
  -d '{"name":"demo-topic","partitions":3,"replicationFactor":1}' | jq .

echo ""
echo "=== C. Interceptor: send and view stats ==="
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-1","itemCount":3}' | jq .
sleep 2
curl -s http://localhost:8081/api/interceptor/stats | jq .

echo ""
echo "=== C. Kafka UI: check headers ==="
echo "Open: http://localhost:8080"
echo "Topics → 24.orders.created → latest message → Headers"
echo "Expected: x-service-name, x-sent-at, x-trace-id"

echo ""
echo "=== D. Quotas (bonus) ==="
docker exec kafka kafka-configs \
  --bootstrap-server localhost:9092 \
  --alter \
  --add-config 'producer_byte_rate=1048576' \
  --entity-type clients \
  --entity-name order-service
echo "Quota set: 1 MB/s"
```

---

## 12. Self-Check Questions

- Which compression type is best for production and why?
- Does compression happen at the record level or the batch level?
- Why is `compression.type=producer` at the broker level recommended?
- When does the `onSend()` interceptor run: before or after serialization?
- What is the difference between ProducerInterceptor and ConsumerInterceptor?
- What happens if `onSend()` returns null?
- What is the difference between the AdminClient API and the kafka-topics CLI?
- Why is `AdminClient.close()` mandatory?