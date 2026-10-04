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

# Branch 21
# Log Compaction

**Apache Kafka for Certification & Production**

---

## Agenda

1. `cleanup.policy=delete` vs `cleanup.policy=compact`
2. How Log Compaction works — the Log Cleaner
3. Tombstone record — deleting a key
4. `cleanup.policy=delete,compact` — combined mode
5. Compaction parameters — tuning the cleaner
6. State Reconstruction — reading from compacted topic
7. Demo: product catalog with compaction
8. Use cases — when to choose compaction
9. Key Takeaways & What's Next

---

## delete vs compact — The Core Difference

**Two fundamentally different retention strategies:**

```
cleanup.policy=delete (default):
  Kafka deletes messages after retention.ms (7 days default)
  ALL messages eventually deleted — old and new alike
  What remains: recent events within the time window

cleanup.policy=compact:
  Kafka keeps ONLY the latest message per key
  Older versions of the same key are deleted by Log Cleaner
  What remains: current state of every key — FOREVER

cleanup.policy=delete,compact (combined):
  Compaction runs continuously (latest value per key)
  PLUS time-based deletion after retention.ms
  What remains: latest value per key within retention window
```

---

## How Log Compaction Works

**Log Cleaner runs in the background, merging segments:**

```
Topic 21.products BEFORE compaction:
  offset=0: key=prod-1, value={"name":"Laptop","price":1000}
  offset=1: key=prod-2, value={"name":"Phone","price":500}
  offset=2: key=prod-1, value={"name":"Laptop","price":950}   ← update
  offset=3: key=prod-3, value={"name":"Tablet","price":300}
  offset=4: key=prod-2, value=null                            ← tombstone
  offset=5: key=prod-1, value={"name":"Laptop Pro","price":920} ← update

Topic 21.products AFTER compaction:
  offset=3: key=prod-3, value={"name":"Tablet","price":300}
  offset=5: key=prod-1, value={"name":"Laptop Pro","price":920}
  (prod-2 removed after delete.retention.ms expires)
```

> Log Cleaner preserves ordering within a key and respects `delete.retention.ms`.

---

## Tombstone Record — Deleting a Key

**A null value signals "delete this key from compacted state":**

```kotlin
// product-catalog-service: mark product as deleted
fun deleteProduct(productId: String) {
    kafkaTemplate.send(
        ProducerRecord("21.products", productId, null)  // ← null value = tombstone
    )
}
```

```
Lifecycle of a tombstone:
  1. Producer sends: key=prod-2, value=null
  2. Consumers reading the topic see tombstone → remove prod-2 from local state
  3. Log Cleaner sees tombstone → removes all older prod-2 records
  4. After delete.retention.ms → tombstone itself is deleted
  5. prod-2 completely gone from the topic
```

> Tombstones are only meaningful in compacted topics with keyed messages.

---

## Compaction Parameters

**Tune the Log Cleaner for your use case:**

```kotlin
// KafkaTopicConfig.kt
TopicBuilder.name("21.products")
    .partitions(3)
    .replicas(1)
    .config(TopicConfig.CLEANUP_POLICY_CONFIG, "compact")
    .config(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.01")  // 1% → aggressive
    .config(TopicConfig.SEGMENT_BYTES_CONFIG, "1048576")            // 1MB segments
    .config(TopicConfig.DELETE_RETENTION_MS_CONFIG, "100")          // tombstone TTL
    .build()
```

| Parameter | Demo value | Production value | Effect |
|-----------|-----------|-----------------|--------|
| `min.cleanable.dirty.ratio` | 0.01 (1%) | 0.5 (50%) | Triggers compaction earlier |
| `segment.bytes` | 1 MB | 1 GB | Smaller = more frequent compaction |
| `delete.retention.ms` | 100 ms | 24 hours | When tombstones are finally removed |

---

## State Reconstruction

**Consumer reads compacted topic from earliest to rebuild state:**

```kotlin
// catalog-reader-service: restore in-memory catalog on startup
@KafkaListener(topics = ["21.products"])
fun handleProduct(record: ConsumerRecord<String, String?>) {
    if (record.value() == null) {
        // tombstone — remove from state
        currentCatalog.remove(record.key())
        tombstoneCount.incrementAndGet()
    } else {
        // latest value — add or update
        currentCatalog[record.key()] = record.value()!!
    }
}
```

```
Reading compacted topic with auto-offset-reset=earliest:
  → receives only LATEST value per key (duplicates already removed)
  → in-memory map built in one pass
  → state = current catalog, not historical log
```

---

## Demo — Product Catalog with Compaction

**Test the full lifecycle:**

```bash
# Create products
curl -X POST http://localhost:8087/api/products \
  -d '{"productId":"prod-1","name":"Laptop","price":1000}'

# Update price
curl -X PUT http://localhost:8087/api/products/prod-1 \
  -d '{"name":"Laptop","price":950}'

# Delete (tombstone)
curl -X DELETE http://localhost:8087/api/products/prod-1

# Check current catalog (reader rebuilt from compacted topic)
curl http://localhost:8088/api/catalog/current
# → prod-1 not present (tombstone processed)

# Mass test
curl -X POST http://localhost:8087/api/demo/chaos?products=50&updates=200
# → 50 products, 200 updates → compaction reduces to 50 records
```

---

## Use Cases — When to Choose Compaction

**Two fundamentally different questions:**

```
cleanup.policy=delete:          cleanup.policy=compact:
  "What happened?"                "What is the current state?"

  Event log                       User settings (latest per userId)
  Audit trail                     Product catalog (latest per productId)
  Time-series metrics             Feature flags (latest per flagId)
  Message queue events            Entity state store (latest per entityId)
  Temporary data                  Configuration store

  Messages expire after           Messages persist indefinitely
  retention.ms                    (always: latest value per key)
```

<br>

> Compacted topics are Kafka's built-in key-value store with changelog semantics.

---

## Key Takeaways

- **`cleanup.policy=compact`** = only latest value per key is retained — forever
- **Log Cleaner** runs in background, merges segments, removes superseded records
- **Tombstone** (`value=null`) = delete signal — key removed after `delete.retention.ms`
- **State reconstruction** = read compacted topic from `earliest` → current state in one pass
- **`min.cleanable.dirty.ratio=0.01`** = aggressive compaction (for demos)
- **Compacted topics** = Kafka's built-in distributed key-value changelog
- **`delete,compact`** = both compaction AND time-based expiry

<br>

> Kafka Streams State Store uses compacted changelog topics internally — same mechanism.

---

## What's Next — Branch 22

**ksqlDB — SQL for Kafka Streams**

- `CREATE STREAM` and `CREATE TABLE` — SQL equivalents of KStream and KTable
- Push query (`EMIT CHANGES`) — continuous real-time results
- Pull query — one-shot point-in-time lookup on a TABLE
- Windowed aggregations: TUMBLING, HOPPING, SESSION in SQL
- `STREAM-TABLE JOIN` — enrich stream with table data via SQL

---

<!-- _class: title -->

## What's Next — Branch 22
- ksqlDB: SQL instead of Java for Kafka Streams
- Push query: live dashboard feed
- Pull query: API endpoint lookup
- Windowed aggregation in 4 lines of SQL

# Questions?

**Branch 21 — Log Compaction**