---
marp: true
theme: default
paginate: true
backgroundColor: #ffffff
style: |
  section {
    font-family: 'Segoe UI', sans-serif;
    font-size: 28px;
  }
  section.title {
    text-align: center;
    justify-content: center;
  }
  h1 { color: #1a1a2e; }
  h2 { color: #16213e; border-bottom: 2px solid #0f3460; padding-bottom: 8px; }
  code { background: #f4f4f4; padding: 2px 6px; border-radius: 4px; }
  pre { background: #1e1e1e; color: #d4d4d4; border-radius: 8px; }
  blockquote { border-left: 4px solid #0f3460; padding-left: 16px; color: #555; }
  .note { font-size: 20px; color: #666; font-style: italic; }
---

<!-- _class: title -->

# Branch 14
# Kafka Streams

**Apache Kafka for Certification & Production**

---

## Agenda

1. What is Kafka Streams — embedded library, not a cluster
2. KStream vs KTable — the fundamental difference
3. `groupBy / count` — stateful aggregation
4. State Store — RocksDB-backed local state
5. Tumbling Windows — time-based aggregation
6. Hopping Windows — overlapping windows (comparison)
7. `@EnableKafkaStreams` — Spring integration
8. Interactive Queries — HTTP access to local state
9. Filter → separate high-value topic
10. Key Takeaways & What's Next

---

## What is Kafka Streams?

**An embedded library — runs inside your application, not a separate cluster:**

```
Traditional Kafka consumer:
  poll() → process one message → commit → poll() again
  Stateless by default
  You manage state (DB, Redis, in-memory)

Kafka Streams:
  Topology of operators: filter, map, groupBy, count, join
  State managed automatically in embedded RocksDB
  Exactly-once processing via changelog topics
  Runs as a JVM library — no extra infrastructure
```

<br>

- No separate "Streams cluster" to deploy
- Scales horizontally: add more instances → more partitions handled
- State automatically replicated to Kafka changelog topics

> Kafka Streams = powerful stateful processing with minimal infrastructure.

---

## KStream vs KTable

**The fundamental distinction in Kafka Streams:**

```
KStream (append-only log):
  offset=0: key=user-1, value={orderId=a, amount=20}
  offset=1: key=user-2, value={orderId=b, amount=50}
  offset=2: key=user-1, value={orderId=c, amount=30}   ← new event, NOT update

  "Every record is an independent event"

KTable (upsert — latest value per key):
  key=user-1 → 2    (after two orders counted)
  key=user-2 → 1

  "Every record is an update to current state"

KTable backed by: State Store (RocksDB local) + Kafka changelog topic
```

> `KStream` = journal of all events. `KTable` = current snapshot of state.

---

## groupBy / count — Stateful Aggregation

**Count orders per userId — the canonical Kafka Streams example:**

```kotlin
val ordersStream: KStream<String, OrderCreatedEvent> =
    builder.stream("14.orders.created", consumed)

// Group by userId and count
val orderCountTable: KTable<String, Long> =
    ordersStream
        .groupBy { _, v -> KeyValue(v.userId.toString(), v) }
        .count(Materialized.`as`("orders-by-user-store"))

// Write results to output topic (changelog)
orderCountTable.toStream().to("14.analytics.orders-by-user")
```

<br>

- `groupBy` → repartitions stream by new key (userId)
- `count()` → maintained in State Store
- Every count update → written to `14.analytics.orders-by-user` topic

---

## State Store — RocksDB-Backed Local State

**Persistent embedded key-value store:**

```
Kafka Streams stores state locally in RocksDB:

  State Store name: "orders-by-user-store"
  Type: key-value store
  Backend: RocksDB (embedded, persistent on disk)

On service restart:
  Step 1: read changelog topic from beginning
  Step 2: replay all updates → rebuild state in RocksDB
  Step 3: resume processing from last committed offset

  NO external database needed for state recovery!
```

<br>

- Changelog topic auto-created: `analytics-service-orders-by-user-store-changelog`
- State survives service restarts

> State Store = Kafka manages your state. No Redis, no DB required.

---

## Tumbling Windows

**Fixed-duration non-overlapping time windows:**

```
TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)):

Events:   ●  ●●  ●    ●  ●●●   ●
time: ──[──0:00──────1:00──]──[──1:00──────2:00──]──►
              window 1: 4          window 2: 4

Each window:
  - independent, no overlap
  - exactly 1 minute duration
  - "WithNoGrace" = no waiting for late-arriving events
```

```kotlin
ordersStream
    .groupBy { _, v -> KeyValue(v.category.toString(), v) }
    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
    .count()
    .toStream()
    .peek { k, v -> log.info("Window [{}]: {} orders", k.window(), v) }
```

---

## Hopping Windows (Comparison)

**Overlapping windows — each event appears in multiple windows:**

```
Hopping Window: size=5min, advance=1min

     [0:00──────5:00)    window 1
          [1:00──────6:00)   window 2
               [2:00──────7:00)  window 3

Event at t=3:00 → appears in window 1, 2, and 3

vs Tumbling:
  [0:00──5:00) [5:00──10:00) [10:00──15:00)
  Each event appears in exactly ONE window

Use Hopping for: moving averages, sliding metrics
Use Tumbling for: per-period billing, independent time buckets
```

---

## @EnableKafkaStreams — Spring Integration

**Spring Kafka manages the Streams application lifecycle:**

```kotlin
@Configuration
@EnableKafkaStreams
class KafkaStreamsConfig {

    @Bean(name = [KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME])
    fun kStreamsConfig(): KafkaStreamsConfiguration {
        return KafkaStreamsConfiguration(mapOf(
            StreamsConfig.APPLICATION_ID_CONFIG         to "analytics-service",
            StreamsConfig.BOOTSTRAP_SERVERS_CONFIG      to bootstrapServers,
            StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG
                                                        to SpecificAvroSerde::class.java,
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
                                                        to schemaRegistryUrl
        ))
    }
}
```

- `APPLICATION_ID_CONFIG` = consumer group ID for Streams app
- Spring manages `start()`, `stop()`, health checks

---

## Interactive Queries — HTTP Access to Local State

**Query the embedded State Store via REST endpoint:**

```kotlin
@GetMapping("/analytics/orders-by-user/{userId}")
fun getOrderCount(@PathVariable userId: String): Long {
    val store = streams.store(
        StoreQueryParameters.fromNameAndType(
            "orders-by-user-store",
            QueryableStoreTypes.keyValueStore<String, Long>()
        )
    )
    return store.get(userId) ?: 0L
}
```

```bash
curl http://localhost:8084/api/analytics/orders-by-user/user-42
# → 5

curl http://localhost:8084/api/analytics/top-users
# → [{"userId":"user-42","count":5}, {"userId":"user-01","count":3}]
```

---

## Key Takeaways

- **Kafka Streams** = stateful stream processing embedded in your JVM application
- **KStream** = append-only log (every event is new)
- **KTable** = current state view (every event is an upsert)
- **State Store** (RocksDB) = automatic state persistence + recovery from changelog
- **Tumbling Windows** = non-overlapping fixed-duration buckets
- **Interactive Queries** = HTTP access to local State Store
- **`@EnableKafkaStreams`** = Spring manages Streams app lifecycle
- No extra cluster required — Streams runs inside your application

<br>

> Kafka Streams turns Kafka from a transport into a stateful processing engine.

---

## What's Next — Branch 15

**Kafka Connect: CDC with Debezium and Elasticsearch**

- Kafka Connect — integration framework (200+ connectors, no code)
- CDC (Change Data Capture) — capture DB changes via PostgreSQL WAL
- Debezium Source Connector — reads WAL, publishes to Kafka
- Elasticsearch Sink Connector — indexes events for search
- SMT `ExtractNewRecordState` — unwrap Debezium envelope

---

<!-- _class: title -->

## What's Next — Branch 15
- Kafka Connect: JSON config, no producer code
- CDC: capture every INSERT/UPDATE/DELETE automatically
- Debezium: PostgreSQL WAL → Kafka topic
- SMT: transform messages in the connector pipeline

# Questions?

**Branch 14 — Kafka Streams**