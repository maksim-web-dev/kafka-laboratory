# Branch 20 — Kafka Streams Advanced: Joins, GlobalKTable, EOS

## 1. What we learn in this branch

- **KStream-KTable Join** — enriching an event stream with data from a table (co-partitioned by key).
- **KStream-GlobalKTable Join** — enrichment without co-partitioning (key extractor lambda).
- **KStream-KStream Windowed Join** — matching two streams within a time window (5 min).
- **GlobalKTable vs KTable** — full replication on every Streams instance vs partitioned.
- **`selectKey()`** — re-keying a stream for co-partitioning before a KTable join.
- **EOS (Exactly-Once Semantics)** — `processing.guarantee=exactly_once_v2`.

---

## 2. Changes compared to the previous branch (branch19)

- **Reverted** to single-broker (no SASL/ACL) — focus is on Kafka Streams advanced operations.
- **Added** user-service — a new microservice for managing user profiles and products.
- **analytics-service** extended: now has 3 join types (KTable, GlobalKTable, KStream-KStream).
- **New topics**: `20.users`, `20.products`, `20.payments.processed`,
  `20.enriched.orders`, `20.enriched.products`, `20.matched.orders`.
- **EOS** enabled in analytics-service: `processing.guarantee: exactly_once_v2`.

---

## 3. Architecture

```
order-service  ──[20.orders.created]─────────────────────────────► analytics-service
               ──[20.payments.processed]─────────────────────────► (Kafka Streams)
user-service   ──[20.users (KTable)]────────────────────────────►       │
               ──[20.products (GlobalKTable)]───────────────────►       │
                                                                         │
                                             [20.enriched.orders]  ◄── KStream-KTable join
                                             [20.enriched.products]◄── KStream-GlobalKTable join
                                             [20.matched.orders]   ◄── KStream-KStream join
```

**Services and ports (from docker-compose-20.yml):**
- `kafka` — 9092 (KRaft single-node)
- `kafka-ui` — 8080 → http://localhost:8080
- `order-service-b20` — 8081 → http://localhost:8081
- `user-service-b20` — 8086 → http://localhost:8086
- `analytics-service-b20` — 8084 → http://localhost:8084

---

## 4. Key Concepts

### KStream-KTable Join (co-partitioning required)

```
Problem:
  orders stream (key=orderId) JOIN users KTable (key=userId)
  → different keys! Re-keying is needed.

Solution: selectKey() before join
  ordersStream
    .selectKey { _, v -> v.userId }     ← re-key by userId
    .join(usersTable) { order, user ->   ← KTable join
        EnrichedOrder(order, user.name, user.tier)
    }
    .to("20.enriched.orders")

Co-partitioning requirement:
  - orders and users MUST have the same number of partitions
  - selectKey() → internal repartition topic (created automatically)
```

### KStream-GlobalKTable Join (no co-partitioning)

```
GlobalKTable:
  - Each Streams instance reads ALL partitions → full copy on every node
  - Co-partitioning is NOT required
  - Key extractor lambda instead of selectKey

ordersStream
  .join(productsGlobalTable,
      { _, order -> order.productId },   ← key extractor: takes productId from order
      { order, product ->
          EnrichedProduct(order, product.name, product.category)
      }
  )
  .to("20.enriched.products")

Difference from KTable:
  KTable: each instance reads its partitions (memory efficient)
  GlobalKTable: each instance reads everything (memory intensive, but no co-partitioning)
```

### KStream-KStream Windowed Join

```
Problem: orders and payments are two streams; we want to match an order with its payment

Solution: JoinWindows defines the time range

ordersStream
  .join(paymentsStream,
      { order, payment -> MatchedOrder(order, payment.status) },
      JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5))
  )
  .to("20.matched.orders")

order t=0:00 ↔ payment t=0:00..4:59 → MATCH ✓
order t=0:00 ↔ payment t=5:01       → NO MATCH ✗

Both streams must have the same key (orderId) → co-partitioned
```

### EOS (Exactly-Once Semantics)

```yaml
spring:
  kafka:
    streams:
      properties:
        processing.guarantee: exactly_once_v2
```

```
exactly_once_v2:
  1. Transactional producer (transactional.id = {app-id}-{partition})
  2. Read-process-write as a single transaction
  3. On failure → transaction is rolled back → message is re-read
  4. Result: each input message is processed exactly once

Vs exactly_once (deprecated): slower (distributed transactions)
Vs at_least_once (default): possible duplicates on failure after write, before commit
```

---

## 5. How to Run

```bash
docker compose -f docker-compose-20.yml up --build

# Kafka UI: http://localhost:8080
```

---

## 6. How to Test

### Step 1 — Populate KTable (users)

```bash
# Add user profiles
for tier in GOLD SILVER BRONZE; do
  for i in 1 2 3; do
    curl -s -X POST http://localhost:8086/api/users \
      -H "Content-Type: application/json" \
      -d "{\"userId\":\"user-$i\",\"name\":\"User $i\",\"email\":\"user$i@example.com\",\"tier\":\"$tier\"}" \
      | jq -r '.userId'
  done
done

# View
curl -s http://localhost:8086/api/users | jq .
```

### Step 2 — KStream-KTable Join (orders ← users)

```bash
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-1","productId":"prod-laptop","quantity":1,"totalAmount":1500.0}' | jq .

# analytics-service logs:
# [JOIN-KTABLE] orderId=... userId=user-1 userName=User 1 userTier=GOLD
# Kafka UI → topic 20.enriched.orders
```

### Step 3 — KStream-GlobalKTable Join (orders ← products)

```bash
# Add product to GlobalKTable
curl -s -X POST http://localhost:8086/api/products \
  -H "Content-Type: application/json" \
  -d '{"productId":"prod-laptop","name":"Laptop Pro","category":"ELECTRONICS"}' | jq .

# Order will be enriched with the product name
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-2","productId":"prod-laptop","quantity":1,"totalAmount":1500.0}' | jq .

# Logs: [JOIN-GLOBAL] orderId=... productId=prod-laptop productName=Laptop Pro
# Kafka UI → topic 20.enriched.products
```

### Step 4 — KStream-KStream Windowed Join

```bash
# order-service immediately publishes the payment as well
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-2","productId":"prod-phone","quantity":2,"totalAmount":800.0}' | jq .

# Logs: [JOIN-STREAM] orderId=... amount=800.0 paymentStatus=APPROVED matchedAt=...
# Kafka UI → topic 20.matched.orders
```

### Step 5 — Batch test

```bash
curl -s -X POST "http://localhost:8081/api/orders/batch?users=user-1,user-2,user-3&count=10" | jq .
```

---

## 7. Deep Dive

### co-partitioning and selectKey()

```
Co-partitioning rule for KTable join:
  - Joined streams MUST have the same number of partitions
  - Joined streams MUST have the same partition key

ordersStream key=orderId, usersTable key=userId
→ selectKey() is required → repartition (internal topic)
→ Kafka Streams automatically routes messages to the correct instance

Without selectKey() → IllegalArgumentException on topology validation
```

### GlobalKTable cost

```
KTable (regular):
  Each instance reads M/N partitions (N = number of instances)
  Memory: state size / N
  Join: only with co-partitioned streams

GlobalKTable:
  Each instance reads ALL partitions
  Memory: full state size on every instance
  Join: with any stream (key extractor)

GlobalKTable is suitable for:
  - small tables (config, products, users)
  - when co-partitioning is inconvenient or impossible
```

### KTable changelog topic

```
KTable backed by internal Kafka topic:
  analytics-service-KSTREAM-KEY-SELECT-... ← repartition topic
  analytics-service-...-STATE-STORE-changelog ← state backup

On restart:
  Streams reads changelog topic → restores RocksDB state store
  → resumes processing without losing state
```

---

## 8. Project Structure

```
branch20_streams_advanced/
├── order-service/
│   ├── controller/OrderController.kt      ← POST /api/orders, POST /api/orders/batch
│   └── service/OrderService.kt            ← publishes 20.orders.created + 20.payments.processed
├── user-service/
│   ├── controller/UserController.kt       ← POST /api/users, POST /api/products
│   └── service/UserService.kt             ← publishes 20.users, 20.products
└── analytics-service/
    ├── config/KafkaStreamsConfig.kt        ← @EnableKafkaStreams, EOS config
    └── streams/
        ├── OrderEnrichmentTopology.kt      ← KStream-KTable join → 20.enriched.orders
        ├── ProductEnrichmentTopology.kt    ← KStream-GlobalKTable join → 20.enriched.products
        └── OrderPaymentMatchTopology.kt    ← KStream-KStream windowed → 20.matched.orders
```

---

## 9. What's Next

- **Branch 21** — Log Compaction: `cleanup.policy=compact`, tombstone records,
  state reconstruction via compacted topic.

---

## 10. Lecture Slides

### Slide 1 — KTable vs GlobalKTable

```
KTable (partitioned):              GlobalKTable:
  instance-1: userId=1..500          instance-1: userId=1..1000 (ALL)
  instance-2: userId=501..1000       instance-2: userId=1..1000 (ALL)

  Memory: N/2 each                   Memory: N each (2x)
  Join: co-partitioned only          Join: any stream (key extractor)
```

### Slide 2 — Windowed Join

```
  orders:   ──●────────────────────────────►
                orderId=A, t=0:00
  payments: ──────────●───────────────────►
                      orderId=A, t=0:03

  JoinWindows(5 min): |0:00 - 0:03| = 3 min < 5 min → MATCH ✓

  Result → 20.matched.orders: {orderId=A, paymentStatus=APPROVED}
```

### Slide 3 — EOS exactly_once_v2

```
at_least_once (default):
  read(offset=5) → process → write(result) → commit(offset=6)
  Crash before commit → re-read offset=5 → duplicate result!

exactly_once_v2:
  Transaction: begin
    read(offset=5) → process → write(result) → commit(offset=6)
  Transaction: commit atomically
  Crash → rollback → re-read offset=5 → idempotent → no duplicate
```

---

## 11. Demo Script

```bash
#!/bin/bash
echo "=== Branch 20: Kafka Streams Advanced Demo ==="

docker compose -f docker-compose-20.yml up --build -d
sleep 90

echo ""
echo "=== Populate KTable (users) ==="
for i in 1 2 3; do
  curl -s -X POST http://localhost:8086/api/users \
    -H "Content-Type: application/json" \
    -d "{\"userId\":\"user-$i\",\"name\":\"User $i\",\"tier\":\"GOLD\"}"
done

echo ""
echo "=== Populate GlobalKTable (products) ==="
curl -s -X POST http://localhost:8086/api/products \
  -H "Content-Type: application/json" \
  -d '{"productId":"prod-laptop","name":"Laptop Pro","category":"ELECTRONICS"}'

echo ""
echo "=== KStream-KTable Join ==="
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-1","productId":"prod-laptop","quantity":1,"totalAmount":1500.0}' | jq .
sleep 3
echo "Kafka UI → topic 20.enriched.orders"

echo ""
echo "=== KStream-GlobalKTable Join ==="
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-2","productId":"prod-laptop","quantity":2,"totalAmount":3000.0}' | jq .
sleep 3
echo "Kafka UI → topic 20.enriched.products"

echo ""
echo "=== KStream-KStream Windowed Join ==="
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-3","productId":"prod-phone","quantity":1,"totalAmount":800.0}' | jq .
sleep 3
echo "Kafka UI → topic 20.matched.orders"
```

---

## 12. Self-Check Questions

- What is co-partitioning and why is it required for a KStream-KTable join?
- What is the difference between KTable and GlobalKTable in terms of memory and join capabilities?
- Why is `selectKey()` needed before a join and what happens after it?
- What is JoinWindows and why does `WithNoGrace` exist?
- What is the difference between `at_least_once` and `exactly_once_v2` on failure?
- What is a KTable changelog topic and how is state restored on restart?
- When should you use GlobalKTable instead of KTable?
- What happens if user-service sends a profile update? Will the KTable be updated?