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

# Branch 20
# Kafka Streams Advanced

**Joins, GlobalKTable, EOS**

**Apache Kafka for Certification & Production**

---

## Agenda

1. Three join types — when to use each
2. `KStream-KTable Join` — co-partitioning requirement
3. `selectKey()` — re-keying for co-partitioned joins
4. `KStream-GlobalKTable Join` — full replication, no co-partitioning
5. `KStream-KStream Windowed Join` — correlate two streams in time
6. `JoinWindows` — defining the time tolerance
7. `GlobalKTable` vs `KTable` — memory vs flexibility
8. EOS: `processing.guarantee=exactly_once_v2`
9. Key Takeaways & What's Next

---

## Three Join Types — When to Use Each

**Kafka Streams supports three join patterns:**

```
KStream-KTable Join:
  Enrich stream events with lookup data (e.g., user profile)
  Requires: same key AND same partition count (co-partitioned)
  Use case: enrich order with user name/tier

KStream-GlobalKTable Join:
  Enrich stream with ANY key (custom key extractor lambda)
  No co-partitioning required
  Trade-off: full table replicated on every Streams instance
  Use case: enrich order with product details (different key)

KStream-KStream Windowed Join:
  Correlate two event streams within a time window
  Both streams must use the same key
  Use case: match order with its payment within 5 minutes
```

---

## KStream-KTable Join

**Enrich orders with user profile data:**

```kotlin
// Problem: orders key=orderId, users key=userId → different keys!
// Solution: selectKey() to re-key orders by userId

val enrichedOrders = ordersStream
    .selectKey { _, order -> order.userId }   // re-key by userId
    .join(usersTable) { order, user ->         // join with KTable
        EnrichedOrder(
            orderId = order.orderId,
            userName = user.name,
            userTier = user.tier,
            totalAmount = order.totalAmount
        )
    }
    .to("20.enriched.orders")
```

<br>

- `selectKey()` triggers an internal repartition topic (automatic)
- After repartition: orders and users share the same key → co-partitioned ✓

---

## selectKey() and Co-partitioning

**Why co-partitioning is required for KTable joins:**

```
Co-partitioning requirement:
  orders and users MUST have the same number of partitions
  Keys MUST be distributed by the same partitioner

Why:
  Streams instance-1 handles partition-0 of both orders and users
  It can only join order[userId=5] with user[userId=5] if they
  are in the SAME partition on BOTH topics

selectKey() + internal repartition:
  orders stream re-partitioned by userId
  → same userId always lands in the same partition
  → KTable lookup is always local (no network calls)
```

> Co-partitioning = locality. Local lookups = no network overhead.

---

## KStream-GlobalKTable Join

**Enrich with product details — no co-partitioning needed:**

```kotlin
// products GlobalKTable: ALL partitions read by EVERY Streams instance
// Key extractor: extract productId from the order event

val enrichedProducts = ordersStream
    .join(
        productsGlobalTable,
        { _, order -> order.productId },    // key extractor: map order → product key
        { order, product ->
            EnrichedProduct(
                orderId = order.orderId,
                productName = product.name,
                category = product.category
            )
        }
    )
    .to("20.enriched.products")
```

<br>

> No `selectKey()` needed — `KeyValueMapper` does the join-key mapping inline.

---

## KTable vs GlobalKTable

**Trade-off: memory efficiency vs join flexibility:**

```
KTable (partitioned):
  Streams instance-1: handles partitions 0-499 of products table
  Streams instance-2: handles partitions 500-999 of products table

  Memory: N/num_instances per instance (efficient)
  Constraint: joining stream MUST be co-partitioned with KTable

GlobalKTable:
  Streams instance-1: reads ALL partitions → full copy of products
  Streams instance-2: reads ALL partitions → full copy of products

  Memory: full table size per instance (N × num_instances total)
  Advantage: join ANY stream key without selectKey()
```

> Use **KTable** for large lookup tables. Use **GlobalKTable** for small reference data.

---

## KStream-KStream Windowed Join

**Match orders with their payments within 5 minutes:**

```kotlin
val matchedOrders = ordersStream
    .join(
        paymentsStream,
        { order, payment ->
            MatchedOrder(orderId = order.orderId, paymentStatus = payment.status)
        },
        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5))
    )
    .to("20.matched.orders")
```

```
order(orderId=A, t=0:00) ↔ payment(orderId=A, t=0:03) → |3 min| < 5 min → MATCH ✓
order(orderId=B, t=0:00) ↔ payment(orderId=B, t=5:01) → |5:01| > 5 min → NO MATCH ✗
```

> Both streams must use `orderId` as key → co-partitioned automatically.

---

## EOS — exactly_once_v2

**Exactly-Once Semantics in Kafka Streams:**

```yaml
# analytics-service application.yml
spring:
  kafka:
    streams:
      properties:
        processing.guarantee: exactly_once_v2
```

```
at_least_once (default):
  read(offset=5) → process → write(result) → commit(offset=6)
  Crash before commit → re-read offset=5 → DUPLICATE result written!

exactly_once_v2:
  Transaction: begin
    read(offset=5) → process → write(result) → commit(offset=6)
  Transaction: commit atomically
  Crash before commit → transaction rollback → re-read from offset=5
  → idempotent → same result → no duplicate ✓

Vs exactly_once (deprecated): slower distributed transaction protocol
Vs at_least_once: possible duplicates on crash
```

---

## Key Takeaways

- **KStream-KTable Join** = enrich stream with table; requires co-partitioned keys
- **`selectKey()`** = re-key stream to satisfy co-partitioning before KTable join
- **KStream-GlobalKTable Join** = flexible key mapping; full table replicated everywhere
- **KStream-KStream Join** = correlate two streams within `JoinWindows` time tolerance
- **`JoinWindows.ofTimeDifferenceWithNoGrace`** = no waiting for late arrivals
- **GlobalKTable** = small reference data without co-partitioning constraint
- **`processing.guarantee=exactly_once_v2`** = transactional read-process-write

<br>

> Join type selection depends on: key relationships, table size, and latency requirements.

---

## What's Next — Branch 21

**Log Compaction — Kafka as a state store**

- `cleanup.policy=compact` — keep only the latest value per key
- Tombstone record (`value=null`) — delete a key from compacted topic
- `cleanup.policy=delete,compact` — compaction + time-based retention
- State reconstruction: consumer reads compacted topic to restore current state
- Use cases: product catalog, user settings, feature flags

---

<!-- _class: title -->

## What's Next — Branch 21
- Log compaction: latest value per key, always
- Tombstone: null value = delete signal
- State reconstruction from compacted topic
- Use cases: catalog, config, entity changelog

# Questions?

**Branch 20 — Kafka Streams Advanced**