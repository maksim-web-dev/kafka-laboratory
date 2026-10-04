---
marp: true
theme: default
paginate: true
backgroundColor: #ffffff
style: |
  section { font-family: 'Segoe UI', sans-serif; font-size: 28px; }
  section.title { text-align: center; justify-content: center; }
  h1 { color: #1a1a2e; }
  h2 { color: #16213e; border-bottom: 2px solid #0f3460; padding-bottom: 8px; }
  code { background: #f4f4f4; padding: 2px 6px; border-radius: 4px; }
  pre { background: #1e1e1e; color: #d4d4d4; border-radius: 8px; }
  blockquote { border-left: 4px solid #0f3460; padding-left: 16px; color: #555; }
---

<!-- _class: title -->

# Branch 24
# Advanced Topics

**Compression · AdminClient · Interceptors**

**Apache Kafka for Certification & Production**

---

## Agenda

1. Compression — types and trade-offs
2. Compression benchmark — measuring the difference
3. Broker-level compression override
4. `AdminClient` API — programmatic cluster management
5. `ProducerInterceptor` — hooks in the send pipeline
6. `HeaderEnrichmentInterceptor` — tracing headers
7. `ConsumerInterceptor` — hooks in the receive pipeline
8. Multiple interceptors — chaining
9. Quotas — rate-limiting per client
10. Key Takeaways — Course Summary

---

## Compression — Types and Trade-offs

**Choose based on CPU budget and message characteristics:**

```
Type      Compression    CPU        Latency    Recommendation
none      0%             minimal    minimal    dev, tiny messages
lz4       40–50%         low        low        production ✓ (recommended)
snappy    40–50%         medium     medium     Google systems (legacy)
zstd      50–60%         medium     medium     production ✓ (best ratio)
gzip      60–70%         high       high       batch archiving
```

```yaml
# application.yml
spring:
  kafka:
    producer:
      compression-type: lz4   # or: gzip, snappy, zstd, none
```

> **`lz4`** — fastest decompression, low CPU overhead, good ratio.
> **`zstd`** — best compression ratio with reasonable CPU (Kafka 2.1+).

---

## Compression Benchmark

**Measure what you actually gain:**

```bash
# Compare all compression types with 1000 messages
curl "http://localhost:8081/api/benchmark/compare?count=1000"
```

```json
{
  "none":   {"throughput": 5200, "avgLatency": 0.19, "totalBytes": 312000},
  "lz4":    {"throughput": 4800, "avgLatency": 0.21, "totalBytes": 187000},
  "snappy": {"throughput": 4600, "avgLatency": 0.22, "totalBytes": 182000},
  "zstd":   {"throughput": 4400, "avgLatency": 0.23, "totalBytes": 165000},
  "gzip":   {"throughput": 3800, "avgLatency": 0.26, "totalBytes": 142000}
}
```

> With JSON payloads: `lz4` gives ~40% size reduction with minimal CPU overhead.
> For large batches: `zstd` wins on ratio. For low-latency: `lz4` or `none`.

---

## Broker-Level Compression

**Producer and broker compression can differ:**

```
compression.type=producer (broker default):
  Broker stores batches in exactly the format sent by producer
  No recompression → efficient → RECOMMENDED

compression.type=gzip (broker overrides):
  Broker recompresses all batches to gzip regardless of producer setting
  → double CPU: decompress + recompress
  → NOT recommended

compression.type=uncompressed (broker overrides):
  Broker always decompresses
  → wastes CPU, no size savings
  → NOT recommended

Best practice:
  Set compression on producer (lz4 or zstd)
  Set broker compression.type=producer (pass-through)
```

---

## AdminClient API — Programmatic Cluster Management

**Manage Kafka without CLI tools — from application code:**

```kotlin
val adminClient = AdminClient.create(mapOf(
    AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092"
))

// List all topics
val topics = adminClient.listTopics().listings().get()
topics.forEach { println("${it.name()} (internal=${it.isInternal})") }

// Create a new topic
adminClient.createTopics(
    listOf(NewTopic("new-topic", 3, 1.toShort()))
).all().get()

// Change topic retention
val resource = ConfigResource(ConfigResource.Type.TOPIC, "24.orders.created")
val op = AlterConfigOp(ConfigEntry("retention.ms", "86400000"), AlterConfigOp.OpType.SET)
adminClient.incrementalAlterConfigs(mapOf(resource to listOf(op))).all().get()

// Describe consumer groups
adminClient.describeConsumerGroups(listOf("my-group")).all().get()
```

---

## ProducerInterceptor — Hooks in Send Pipeline

**Intercept every message before it leaves the producer:**

```kotlin
class MetricsProducerInterceptor : ProducerInterceptor<String, Any> {
    private val sentCount   = AtomicLong(0)
    private val ackCount    = AtomicLong(0)
    private val errorCount  = AtomicLong(0)

    // Called BEFORE serialization and sending
    // Can MODIFY the record (add headers, change key, etc.)
    override fun onSend(record: ProducerRecord<String, Any>): ProducerRecord<String, Any> {
        sentCount.incrementAndGet()
        return record  // MUST return record (modified or original)
    }

    // Called AFTER receiving ACK from broker (or on error)
    override fun onAcknowledgement(metadata: RecordMetadata?, exception: Exception?) {
        if (exception == null) ackCount.incrementAndGet()
        else errorCount.incrementAndGet()
    }
}
```

---

## HeaderEnrichmentInterceptor — Distributed Tracing

**Add correlation headers to every outgoing message:**

```kotlin
class HeaderEnrichmentInterceptor : ProducerInterceptor<String, Any> {
    override fun onSend(record: ProducerRecord<String, Any>): ProducerRecord<String, Any> {
        record.headers()
            .add("x-service-name", "order-service".toByteArray())
            .add("x-sent-at",      System.currentTimeMillis().toString().toByteArray())
            .add("x-trace-id",     UUID.randomUUID().toString().toByteArray())
        return record
    }
    override fun onAcknowledgement(metadata: RecordMetadata?, exception: Exception?) {}
    override fun close() {}
    override fun configure(configs: Map<String, *>) {}
}
```

<br>

> Headers are visible in Kafka UI → trace message flow across services without log correlation.

---

## ConsumerInterceptor — Hooks in Receive Pipeline

**Intercept every message before it reaches the listener:**

```kotlin
class MetricsConsumerInterceptor : ConsumerInterceptor<String, Any> {

    // Called AFTER poll() returns messages, BEFORE listener processes them
    override fun onConsume(records: ConsumerRecords<String, Any>): ConsumerRecords<String, Any> {
        records.forEach { record ->
            val sentAt = record.headers().lastHeader("x-sent-at")
                ?.value()?.let { String(it).toLong() }
            val latencyMs = sentAt?.let { System.currentTimeMillis() - it }
            log.info("Received: topic={}, partition={}, e2e-latency={}ms",
                record.topic(), record.partition(), latencyMs)
        }
        return records  // MUST return records
    }

    // Called AFTER offset commit
    override fun onCommit(offsets: Map<TopicPartition, OffsetAndMetadata>) {
        log.debug("Committed offsets: {}", offsets)
    }
}
```

---

## Key Takeaways

- **`lz4`** = best balance for production (low CPU, good ratio, fast decompress)
- **`zstd`** = best compression ratio (Kafka 2.1+, medium CPU)
- **`compression.type=producer`** on broker = pass-through, no double compression
- **`AdminClient`** = runtime topic management, CI/CD automation, self-service platforms
- **`ProducerInterceptor.onSend()`** = modify records before sending (add headers, metrics)
- **`ProducerInterceptor.onAcknowledgement()`** = track success/error rates after ACK
- **`ConsumerInterceptor.onConsume()`** = measure end-to-end latency from headers
- Quotas = `producer_byte_rate` / `consumer_byte_rate` per client for multi-tenancy

<br>

> Interceptors = cross-cutting concerns (metrics, tracing) without changing business logic.

---

## Course Summary — 24 Branches

**From zero to production-ready Kafka engineer:**

```
Foundations (01–05):   Topics, Partitions, Keys, Consumer Groups
Reliability  (06–09):  Manual Commit, Producer Config, Consumer Tuning, DLT
Serialization(10–11):  JSON Type Mapping, Schema Registry + Avro
Guarantees   (12–13):  Transactions (EOS), Saga Pattern
Streams      (14,20):  Kafka Streams, Joins, Windows, EOS
Integration  (15,22):  Kafka Connect (CDC), ksqlDB
Infrastructure(16–19): Cluster HA, Observability, SASL/ACL, Production Demo
Advanced     (21,23,24):Log Compaction, SSL/TLS, Compression+AdminClient
```

> You now have every tool needed for the CCDAK exam and real production systems.

---

<!-- _class: title -->

## Apache Kafka for Certification & Production

**24 branches · 24 topics · 1 complete system**

Order → Payment → Inventory → Notification

# Thank You!

**kafka-laboratory — github.com/your-repo**