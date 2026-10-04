# Branch 14 — Kafka Streams

## 1. What we learn in this branch

- **KStream** — unbounded event stream, record-by-record processing.
- **KTable** — changelog stream: stores only the latest state per key (analogous to a materialized view).
- **groupBy / count** — regrouping the stream and counting events.
- **Tumbling Window** — fixed, non-overlapping time interval (no overlap).
- **State Store** — RocksDB-backed local state storage, accessible via **Interactive Queries**.
- **`@EnableKafkaStreams`** — Spring Kafka automatically manages the Streams application lifecycle.

---

## 2. Changes compared to the previous branch (branch13)

- **Replaced** notification-service with analytics-service using **Kafka Streams** processing.
- **Removed** inventory-service and compensating saga topics.
- **New topics**: `14.analytics.orders-by-user` (KTable results) and
  `14.analytics.orders-high-value` (filtered events).
- **New dependencies**: `kafka-streams`, `kafka-streams-avro-serde`.
- **order-service** added `category` and `totalAmount` fields to OrderCreatedEvent.

---

## 3. Architecture

```
order-service ──[14.orders.created]──► analytics-service (Kafka Streams)
                                               │
                   ┌───────────────────────────┼──────────────────────────────┐
                   │                           │                              │
             [count by userId]          [1-min window               [filter >= $50]
             groupBy(userId)             per category]                        │
             .count()                   .windowedBy()                         │
             KTable                     .count()                              ▼
             state store                peek + log            [14.analytics.orders-high-value]
                   │
                   ▼
     [14.analytics.orders-by-user]
     (changelog: userId → total count)
```

**5-min window — revenue sum per userId:**
```
orders.created
  .groupBy(userId)
  .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
  .aggregate({ 0.0 }, { _, v, agg -> agg + v.totalAmount })
  .toStream()
  .peek(log)
```

**Services and ports (from docker-compose-14.yml):**
- `kafka` — 9092 (KRaft single-node)
- `schema-registry-b14` — 8090
- `kafka-ui` — 8080 → http://localhost:8080
- `order-service-b14` — 8081 → http://localhost:8081
- `analytics-service-b14` — 8084 → http://localhost:8084

---

## 4. Key Concepts

### KStream vs KTable

```
KStream — append-only:
  offset=0: key=user-1, value={orderId=a}
  offset=1: key=user-2, value={orderId=b}
  offset=2: key=user-1, value={orderId=c}  ← new event, not an update

KTable — upsert (latest per key):
  key=user-1 → 2  (after two orders)
  key=user-2 → 1

KTable == "snapshot of current state", KStream == "log of all events"
```

### State Store (RocksDB)

```
Kafka Streams stores state locally:
  Type: RocksDB (embedded key-value)
  Name: "orders-by-user-store"
  Recovery: on restart reads changelog topic from the beginning

Access via Interactive Queries:
  val store = streams.store(
      StoreQueryParameters.fromNameAndType(
          "orders-by-user-store",
          QueryableStoreTypes.keyValueStore()
      )
  )
  store.get("user-1")  // → 5L
```

### Tumbling Window

```
TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)):

[0:00─1:00)  [1:00─2:00)  [2:00─3:00)  ...
   3 orders     5 orders     2 orders

Each window is independent, no overlap.
"WithNoGrace" = do not wait for late-arriving events.

Hopping Window (for comparison):
TimeWindows.of(Duration.ofMinutes(5)).advanceBy(Duration.ofMinutes(1))
  → windows overlap; a new window starts every minute
```

### @EnableKafkaStreams + StreamsConfig

```kotlin
@Configuration
@EnableKafkaStreams
class KafkaStreamsConfig {
    @Bean(name = [KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME])
    fun kStreamsConfig(): KafkaStreamsConfiguration {
        return KafkaStreamsConfiguration(mapOf(
            StreamsConfig.APPLICATION_ID_CONFIG to "analytics-service",
            StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG to SpecificAvroSerde::class.java,
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryUrl
        ))
    }
}
```

`APPLICATION_ID_CONFIG` — application identifier. Kafka Streams automatically creates
internal topics: `analytics-service-orders-by-user-store-changelog`.

### Filter → new branch of the stream

```kotlin
// High-value orders → separate topic
ordersStream
    .filter { _, v -> v.totalAmount >= 50.0 }
    .to("14.analytics.orders-high-value")

// No state store — stateless operation, very efficient
```

---

## 5. How to Run

```bash
# Start
docker compose -f docker-compose-14.yml up --build

# Kafka UI: http://localhost:8080
# analytics-service REST: http://localhost:8084
```

**Readiness check:**
```bash
# Verify Kafka Streams has reached RUNNING state
curl -s http://localhost:8084/api/analytics/orders-by-user | jq .
# {} (empty before the first order)
```

---

## 6. How to Test

### Step 1 — Send a batch of orders

```bash
# 20 orders for 3 users
curl -s -X POST "http://localhost:8081/api/orders/batch?users=alice,bob,charlie&count=20"
```

### Step 2 — State Store: count per userId

```bash
curl -s http://localhost:8084/api/analytics/orders-by-user | jq .
# {"alice": 7, "bob": 8, "charlie": 5}  (distribution depends on rng)
```

### Step 3 — Top-3 users

```bash
curl -s "http://localhost:8084/api/analytics/top-users?limit=3" | jq .
# [{"userId":"bob","count":8}, {"userId":"alice","count":7}, ...]
```

### Step 4 — Tumbling window in Kafka UI

```bash
# Open http://localhost:8080 → Topics → 14.analytics.orders-by-user
# Messages: key=alice, value=7 (updated on each new order)

# Add one order with totalAmount >= $50
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"alice","itemCount":3,"category":"ELECTRONICS","totalAmount":150.0}' | jq .

# Topic 14.analytics.orders-high-value will receive this event
```

### Step 5 — View analytics-service logs

```bash
docker logs analytics-service-b14 -f --tail 30
# [WINDOW-1MIN] category=ELECTRONICS count=5 window=[0:00-1:00)
# [WINDOW-5MIN] userId=alice revenue=450.0 window=[0:00-5:00)
# [HIGH-VALUE] orderId=... totalAmount=150.0
```

---

## 7. Deep Dive

### Topology internally

```
Source → ... → Sink
analytics-service topology has 4 branches:
  1. Source(14.orders.created)
     → SelectKey(userId)
     → GroupBy
     → Count          → StateStore("orders-by-user-store")
     → ToStream
     → Sink(14.analytics.orders-by-user)

  2. Source(14.orders.created)
     → SelectKey(category)
     → GroupBy
     → WindowedBy(1 min)
     → Count
     → ToStream
     → Peek(log)

  3. Source(14.orders.created)
     → SelectKey(userId)
     → GroupBy
     → WindowedBy(5 min)
     → Aggregate(sum totalAmount)
     → ToStream
     → Peek(log)

  4. Source(14.orders.created)
     → Filter(totalAmount >= 50)
     → Sink(14.analytics.orders-high-value)
```

### Co-partitioning requirement

```
KStream-KTable join requires co-partitioning:
  - same number of partitions
  - same key (for routing to the same Streams instance)

In our example groupBy(userId) re-keys the stream → new internal topic
Kafka Streams automatically creates: analytics-service-KSTREAM-AGGREGATE-STATE-STORE-...
```

### Exactly-once via state store

```
On Streams instance failure:
  1. Restart → reads changelog topic from the beginning
  2. Restores RocksDB state store to the last state
  3. Resumes processing from the committed offset

→ Result: at-least-once with idempotent count (count is not duplicated due to KTable semantics)
```

---

## 8. Project Structure

```
branch14_streams/
├── order-service/
│   ├── controller/OrderController.kt         ← POST /api/orders, POST /api/orders/batch
│   └── src/main/avro/OrderCreatedEvent.avsc  ← added: category, totalAmount
└── analytics-service/
    ├── config/
    │   ├── KafkaStreamsConfig.kt              ← @EnableKafkaStreams, StreamsConfig
    │   └── KafkaTopicConfig.kt               ← output topic creation
    ├── streams/
    │   └── OrderAnalyticsTopology.kt         ← full Streams topology (4 branches)
    └── controller/AnalyticsController.kt     ← GET /api/analytics/orders-by-user
                                                 GET /api/analytics/top-users
```

---

## 9. What's Next

- **Branch 15** — Kafka Connect: instead of a Spring Kafka consumer — Debezium CDC connector
  reads changes from the PostgreSQL WAL and publishes to Kafka without any code.
- **Branch 20** — Kafka Streams Advanced: KStream-KTable join, KStream-GlobalKTable join,
  KStream-KStream windowed join, EOS (exactly_once_v2).

---

## 10. Lecture Slides

### Slide 1 — KStream vs KTable

```
KStream (append-only):           KTable (upsert):
  [a, 1]                           user-1 → 2
  [b, 1]     groupBy.count()  →    user-2 → 1
  [a, 1]  ──────────────────►
           (changelog topic)

KTable backed by state store (RocksDB)
```

### Slide 2 — Tumbling Window

```
  events:  ●   ●  ●●   ●     ●●●   ●  ●
time: ─────[──0:00─────1:00──]────[──1:00─────2:00──]────►
             window 1: 4        window 2: 4

TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1))
"WithNoGrace" = no waiting for late arrivals
```

### Slide 3 — Interactive Queries

```
Kafka Streams stores state locally (RocksDB):

HTTP Request → analytics-service → store.get("user-1") → 5L
                                         ↑
                              RocksDB state store
                              (restored from Kafka changelog on restart)
```

---

## 11. Demo Script

```bash
#!/bin/bash
echo "=== Branch 14: Kafka Streams Demo ==="

docker compose -f docker-compose-14.yml up --build -d
echo "Waiting 2 min..."
sleep 120

echo ""
echo "=== Step 1: Generate orders ==="
curl -s -X POST "http://localhost:8081/api/orders/batch?users=alice,bob,charlie&count=15"
sleep 5

echo ""
echo "=== Step 2: State Store — count per user ==="
curl -s http://localhost:8084/api/analytics/orders-by-user | jq .

echo ""
echo "=== Step 3: Top-3 users ==="
curl -s "http://localhost:8084/api/analytics/top-users?limit=3" | jq .

echo ""
echo "=== Step 4: High-value order ==="
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"vip","itemCount":5,"category":"ELECTRONICS","totalAmount":999.0}'
sleep 2
echo "Kafka UI → topic 14.analytics.orders-high-value"

echo ""
echo "=== Logs: Tumbling window aggregations ==="
docker logs analytics-service-b14 --tail 20
```

---

## 12. Self-Check Questions

- What is the semantic difference between KStream and KTable?
- What is a state store and where is it physically stored?
- Why can `groupBy` in Kafka Streams cause re-partitioning?
- What happens to the state store when analytics-service restarts?
- Tumbling window vs Hopping window — what is the difference?
- Why does `WithNoGrace` exist in `TimeWindows.ofSizeWithNoGrace()`?
- What are Interactive Queries and how do they work in multi-instance deployments?
- Why does analytics-service not need Schema Registry when reading if it is already reading Avro?