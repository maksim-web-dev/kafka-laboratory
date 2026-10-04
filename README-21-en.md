# Branch 21 — Log Compaction

## 1. What we learn in this branch

- **`cleanup.policy=compact`** — Kafka keeps only the latest value per key (instead of all events).
- **Tombstone record** — null value → a signal to delete a key from a compacted topic.
- **`cleanup.policy=delete,compact`** — combined mode: both compaction and time-based retention.
- **Compaction parameters** — `min.cleanable.dirty.ratio`, `segment.bytes`, `delete.retention.ms`.
- **State Reconstruction** — a consumer with `auto-offset-reset=earliest` restores current state
  from a compacted topic.
- **Use cases** — product catalog, configuration, entity state (entity changelog).

---

## 2. Changes compared to the previous branch (branch20)

- **Replaced** the order flow with a product-catalog scenario.
- **New services**: product-catalog-service and catalog-reader-service (no order-service).
- **Topic** `21.products` with `cleanup.policy=compact` instead of the default `delete`.
- **Removed** Kafka Streams — focus is on the compaction mechanism at the broker level.
- **New API**: PUT for updates, DELETE for tombstone, `/demo/chaos` for a bulk test.

---

## 3. Architecture

```
product-catalog-service ──[POST /api/products]──► topic: 21.products (compacted)
                         ──[PUT  /api/products/{id}]──► publish updated product
                         ──[DELETE /api/products/{id}]──► publish TOMBSTONE (null value)

catalog-reader-service ──[21.products]──► in-memory current state
                        ──[GET /api/catalog/current]──► latest value per key
                        ──[GET /api/catalog/stats]──► tombstones, total counts
```

**How compaction works:**
```
Topic 21.products (BEFORE compaction):
  key=prod-1, value={"name":"Laptop","price":1000}   offset=0
  key=prod-2, value={"name":"Phone","price":500}      offset=1
  key=prod-1, value={"name":"Laptop","price":950}     offset=2  ← update
  key=prod-3, value={"name":"Tablet","price":300}     offset=3
  key=prod-2, value=null                              offset=4  ← tombstone (delete)
  key=prod-1, value={"name":"Laptop Pro","price":920} offset=5  ← update

Topic 21.products (AFTER compaction):
  key=prod-1, value={"name":"Laptop Pro","price":920} offset=5  ← only the latest version
  key=prod-3, value={"name":"Tablet","price":300}     offset=3
  (prod-2 removed via tombstone + delete.retention.ms)
```

**Services and ports (from docker-compose-21.yml):**
- `kafka` — 9092 (KRaft, `KAFKA_LOG_CLEANER_ENABLE=true`)
- `kafka-ui` — 8080 → http://localhost:8080
- `product-catalog-b21` — 8087 → http://localhost:8087
- `catalog-reader-b21` — 8088 → http://localhost:8088

---

## 4. Key Concepts

### cleanup.policy=compact vs delete

```
cleanup.policy=delete (default):
  Deletes messages after retention.ms (7 days by default)
  Use case: event log, audit trail, temporal data

cleanup.policy=compact:
  Keeps ONLY the latest message per key
  Older versions are removed by the Log Cleaner
  Use case: changelog, state store, reference data (products, configs)

cleanup.policy=delete,compact:
  Compaction + deletion after retention.ms
  Use case: entity state with a limited retention period
```

### Compaction topic parameters

```kotlin
// KafkaTopicConfig.kt
TopicBuilder.name("21.products")
    .partitions(3)
    .replicas(1)
    .config(TopicConfig.CLEANUP_POLICY_CONFIG, "compact")
    .config(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.01") // 1% dirty → aggressive
    .config(TopicConfig.SEGMENT_BYTES_CONFIG, "1048576")           // 1MB (small for demo)
    .config(TopicConfig.DELETE_RETENTION_MS_CONFIG, "100")         // remove tombstone after 100ms
    .build()
```

- `min.cleanable.dirty.ratio=0.01` — compaction triggers when 1% of the log is dirty (for demo)
- `segment.bytes=1MB` — small segments → faster compaction (in production: 1GB)
- `delete.retention.ms=100` — tombstone is deleted after 100ms (in production: 24h)

### Tombstone record

```kotlin
// product-catalog-service
fun deleteProduct(productId: String) {
    kafkaTemplate.send(ProducerRecord("21.products", productId, null))
    // null value = tombstone
}
```

After compaction, the `productId` key disappears from the topic (after `delete.retention.ms`).
A catalog-reader-service reading with `auto-offset-reset=earliest` will see the tombstone
and remove the product from its in-memory map.

### State Reconstruction

```kotlin
// catalog-reader-service
@KafkaListener(topics = ["21.products"])
fun handleProduct(record: ConsumerRecord<String, String?>) {
    if (record.value() == null) {
        // tombstone
        currentCatalog.remove(record.key())
        tombstoneCount.incrementAndGet()
    } else {
        currentCatalog[record.key()] = record.value()!!
    }
}
```

On startup, catalog-reader-service reads the topic from `earliest`:
- Receives only the latest version of each product (compacted)
- Restores the in-memory catalog state

---

## 5. How to Run

```bash
docker compose -f docker-compose-21.yml up --build

# Kafka UI: http://localhost:8080
```

---

## 6. How to Test

### Step 1 — Seed the catalog

```bash
curl -s -X POST http://localhost:8087/api/products/demo/seed | jq .
# Creates 5 products: prod-1 to prod-5
```

### Step 2 — Multiple versions of one product

```bash
# prod-1: initial price 1000
curl -s http://localhost:8087/api/products/prod-1 | jq .

# Update price three times
curl -s -X PUT http://localhost:8087/api/products/prod-1 \
  -H "Content-Type: application/json" -d '{"price":950.0}' | jq .
curl -s -X PUT http://localhost:8087/api/products/prod-1 \
  -H "Content-Type: application/json" -d '{"price":920.0}' | jq .
curl -s -X PUT http://localhost:8087/api/products/prod-1 \
  -H "Content-Type: application/json" -d '{"price":899.0}' | jq .

# Kafka UI: topic 21.products contains 4 records for prod-1 (before compaction)
# After compaction, only 1 remains (price 899)
```

### Step 3 — Tombstone

```bash
curl -s -X DELETE http://localhost:8087/api/products/prod-2
# Publishes null value → prod-2 will disappear after compaction
```

### Step 4 — Chaos demo

```bash
curl -s -X POST http://localhost:8087/api/products/demo/chaos | jq .
# 3 products × 5 updates + 1 delete
# Ideal for observing compaction
```

### Step 5 — Current state from reader-service

```bash
curl -s http://localhost:8088/api/catalog/current | jq .
# {"prod-1":{"name":"Laptop Pro","price":899.0}, ...}
# prod-2 absent (tombstone)

curl -s http://localhost:8088/api/catalog/stats | jq .
# {"totalReceived": N, "tombstones": M, "currentSize": K}
```

### Step 6 — CLI compaction inspection

```bash
# All messages (including tombstones)
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic 21.products \
  --from-beginning \
  --property print.key=true \
  --property print.value=true \
  --timeout-ms 3000

# Topic configuration
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe --topic 21.products
# cleanup.policy=compact, min.cleanable.dirty.ratio=0.01, ...

# Log segments (compaction at the file level)
docker exec kafka ls /var/lib/kafka/data/21.products-0/
```

---

## 7. Deep Dive

### How the Log Cleaner works

```
Log = a set of Segments (files):
  [segment-0.log] [segment-1.log] [active-segment.log]
       (clean)          (dirty)        (active, never compacted)

Log Cleaner:
  1. Determines dirty ratio: dirty_log_size / total_log_size
  2. If dirty ratio > min.cleanable.dirty.ratio → compaction
  3. Reads dirty segment → builds map {key → latest_offset}
  4. Removes records where offset is not the latest for their key
  5. Writes the compacted segment

Active segment is NEVER compacted (only closed segments are)
```

### Compaction guarantees

```
Compaction does NOT guarantee:
  - Immediate removal of old versions
  - That no old versions exist before compaction

Compaction DOES guarantee:
  - After compaction: for each key there is AT LEAST ONE record (the latest)
  - "Tail" (old segments) is compacted
  - "Head" (new records) is not yet compacted

Consumer with earliest offset:
  May see old versions too (if not yet compacted)
  After compaction: only the latest version of each key
```

### Compacted topic as an event sourcing store

```
On every restart:
  1. catalog-reader-service: auto-offset-reset=earliest
  2. Reads the entire compacted topic
  3. Restores in-memory state
  4. Ready to process new events

→ Compacted topic = persistent event store for state reconstruction
→ No need for an external DB for current state
```

---

## 8. Project Structure

```
branch21_log_compaction/
├── product-catalog-service/
│   ├── controller/ProductController.kt   ← POST, PUT, DELETE /api/products
│   │                                        POST /api/products/demo/seed
│   │                                        POST /api/products/demo/chaos
│   ├── service/ProductCatalogService.kt  ← publish to 21.products (with tombstone)
│   └── config/KafkaTopicConfig.kt        ← TopicBuilder with compact policy
└── catalog-reader-service/
    ├── listener/ProductUpdateListener.kt ← @KafkaListener, handles null (tombstone)
    ├── service/CatalogStateService.kt    ← in-memory ConcurrentHashMap
    └── controller/CatalogController.kt   ← GET /api/catalog/current, /stats
```

---

## 9. What's Next

- **Branch 22** — ksqlDB: SQL engine on top of Kafka Streams; CREATE STREAM, TABLE, push/pull queries.

---

## 10. Lecture Slides

### Slide 1 — delete vs compact

```
cleanup.policy=delete:              cleanup.policy=compact:
  [key=A, v=1]  ← will be deleted    [key=A, v=1]  ← will be deleted
  [key=A, v=2]  ← will be deleted    [key=A, v=2]  ← will be deleted
  [key=A, v=3]  ← kept for 7 days    [key=A, v=3]  ← KEPT FOREVER
  [key=B, v=1]  ← kept for 7 days    [key=B, v=1]  ← KEPT FOREVER

  After 7 days: all deleted           Always: only the latest version per key
```

### Slide 2 — Tombstone

```
producer.send(key="prod-2", value=null)   ← tombstone

Before compaction:    After compaction:    After delete.retention.ms:
  [prod-2, v=1]        [prod-2, null]       (nothing — key is removed)
  [prod-2, v=2]
  [prod-2, null]
```

### Slide 3 — Use Cases

```
cleanup.policy=delete:        cleanup.policy=compact:
  - Event log                   - User settings
  - Audit trail                 - Product catalog
  - Time-series metrics         - Feature flags
  - Message queue               - Entity state store
  "What happened?"              "What is the current state?"
```

---

## 11. Demo Script

```bash
#!/bin/bash
echo "=== Branch 21: Log Compaction Demo ==="

docker compose -f docker-compose-21.yml up --build -d
sleep 60

echo ""
echo "=== Step 1: Seed catalog ==="
curl -s -X POST http://localhost:8087/api/products/demo/seed | jq .

echo ""
echo "=== Step 2: Multiple versions of prod-1 ==="
for price in 950 920 899; do
  curl -s -X PUT http://localhost:8087/api/products/prod-1 \
    -H "Content-Type: application/json" -d "{\"price\":$price.0}" | jq .price
done

echo ""
echo "=== Step 3: Tombstone for prod-2 ==="
curl -s -X DELETE http://localhost:8087/api/products/prod-2
echo "prod-2 deleted (tombstone)"

echo ""
echo "=== Step 4: Chaos demo ==="
curl -s -X POST http://localhost:8087/api/products/demo/chaos | jq .

echo ""
echo "=== Current reader state ==="
curl -s http://localhost:8088/api/catalog/current | jq .
curl -s http://localhost:8088/api/catalog/stats | jq .

echo ""
echo "=== CLI: topic configuration ==="
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic 21.products
```

---

## 12. Self-Check Questions

- What is the difference between `cleanup.policy=delete` and `cleanup.policy=compact`?
- What is a tombstone record and when is it deleted?
- Why is `min.cleanable.dirty.ratio=0.01` used for demo but 0.5 for production?
- What is the active segment and why is it never compacted?
- How does catalog-reader-service restore state after a restart?
- When is a compacted topic better than a regular relational DB for storing state?
- What happens if a consumer reads a topic during compaction?
- What is `cleanup.policy=delete,compact` and when should it be used?