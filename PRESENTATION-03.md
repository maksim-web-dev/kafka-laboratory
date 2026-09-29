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

# Branch 03
# Message Keys

**Apache Kafka for Certification & Production**

---

## Agenda

1. The problem with `orderId` as a key
2. Partition selection algorithm — Murmur2 hash
3. `userId` as the right key
4. Ordering guarantee within a partition
5. `null` key → StickyPartitioner
6. Parallelism vs Ordering trade-off
7. Demo: keyed batch vs round-robin
8. `record.key()` in the consumer
9. Key takeaways & CCDAK

---

## The Problem with orderId as a Key

**In branch02 key = `orderId` — a unique UUID per order**

```
OrderCreated(orderId=order-A, userId=user-42)   → partition 2
OrderCreated(orderId=order-B, userId=user-42)   → partition 0
OrderCancelled(orderId=order-A, userId=user-42) → partition 2
```

- Different orders of the same user → **different partitions**
- In a multi-consumer scenario: one user's events handled by different instances
- Cross-order dependencies (credit limit, saga) → ordering not guaranteed

> The problem is NOT within one order — it's between orders of the same user.

---

## Partition Selection Algorithm

**Kafka DefaultPartitioner → Murmur2 hash**

```
partition = Math.abs(murmur2(key.getBytes())) % numPartitions

key="user-42"  →  murmur2  →  abs % 3  =  1  (ALWAYS)
key="user-99"  →  murmur2  →  abs % 3  =  0  (ALWAYS)
key=null       →  StickyPartitioner  →  round-robin
```

<br>

**Determinism:**
- Same key → same hash → same partition
- Independent of time, broker restart, or producer restart

> Determinism = predictable routing = guaranteed ordering.

---

## userId as the Right Key

**With `userId` as key — all events of one user land in the same partition:**

```
OrderCreated(order-A, user-42)   → hash("user-42") % 3 = 1 → partition 1
OrderCreated(order-B, user-42)   → hash("user-42") % 3 = 1 → partition 1
OrderCancelled(order-A, user-42) → hash("user-42") % 3 = 1 → partition 1
```

<br>

- **Ordering** across all orders of user-42 — guaranteed
- **Multi-consumer (branch04+):** all user-42 events → **one** consumer instance
- **Rule:** key = identifier of the entity whose ordering matters

---

## Ordering Guarantee Within a Partition

**Kafka guarantees ordering ONLY within a single partition**

```
Partition 0: [order-A user-99] → [order-C user-99] → [order-E user-99]
Partition 1: [order-B user-42] → [order-D user-42] → [order-F user-42]
Partition 2: [order-G user-17] → [order-H user-17]
```

<br>

- user-42: strict order B → D → F ✅
- Between user-42 and user-99: parallel processing, no ordering guarantee

> Between different partitions — ordering is never guaranteed.

---

## null key → StickyPartitioner

**When key = null, Kafka 2.4+ uses StickyPartitioner:**

```
batch 1: msg1, msg2, msg3  →  all to partition 0  (sticky)
batch 2: msg4, msg5        →  all to partition 2  (new batch)
batch 3: msg6              →  to partition 1
```

<br>

| Strategy | Behavior | Ordering |
|----------|----------|----------|
| Key present | `murmur2(key) % N` | Guaranteed within partition |
| `null` (2.4+) | StickyPartitioner | Not guaranteed |
| `null` (pre-2.4) | Round-robin | Not guaranteed |

---

## Parallelism vs Ordering

**The key trade-off when choosing partition count:**

```
1 partition:  strict global ordering         →  1 consumer max
3 partitions: ordering within each partition →  up to 3 consumers
N partitions: higher throughput              →  up to N consumers
```

<br>

- More partitions = higher throughput = more parallelism
- But different events of one entity may be handled by different consumers
- **Solution:** choose the key so the same entity always routes to the same partition

> Partition count is set upfront — changing it breaks existing key routing.

---

## Demo — Keyed Batch

**`POST /api/orders/demo/keyed` → userId=user-42, count=6**

```json
[
  {"orderId":"uuid-1","userId":"user-42","key":"user-42","partition":1,"offset":0},
  {"orderId":"uuid-2","userId":"user-42","key":"user-42","partition":1,"offset":1},
  {"orderId":"uuid-3","userId":"user-42","key":"user-42","partition":1,"offset":2}
]
```

- All 6 messages → **partition 1**
- `hash("user-42") % 3 = 1` — computed once, always stable
- Kafka UI: **Key** column = `"user-42"`, **Partition** column = `1`

---

## Demo — Round-Robin Batch

**`POST /api/orders/demo/round-robin` → key=null, count=6**

```json
[
  {"orderId":"uuid-1","key":null,"partition":0,"offset":0},
  {"orderId":"uuid-2","key":null,"partition":1,"offset":6},
  {"orderId":"uuid-3","key":null,"partition":2,"offset":0}
]
```

<br>

- Messages distributed across **0, 1, 2**
- Kafka UI: **Key** column is empty
- Same userId, but no key → ordering not guaranteed

---

## record.key() in the Consumer

**The consumer sees the producer's routing decision via `record.key()`**

```kotlin
@KafkaListener(topics = ["03.orders.created"])
fun handleOrderCreated(record: ConsumerRecord<String, OrderCreatedEvent>) {
    log.info("║  Key: {}  →  Partition: {}  Offset: {}",
        record.key(), record.partition(), record.offset())
}
```

Log for a keyed message:
```
║  Key: user-42  →  Partition: 1  Offset: 0
```

Log for a null-key message:
```
║  Key: null  →  Partition: 2  Offset: 0
```

> Useful for debugging: shows exactly why a message landed in that partition.

---

## Key Takeaways

- **Murmur2 hash** — partition selection algorithm; deterministic and stable
- **userId > orderId** — all events of one user in one partition, one consumer instance
- **Ordering** is guaranteed ONLY within a single partition
- **null key** → StickyPartitioner (Kafka 2.4+): even distribution, no ordering
- **Hot partition** — anti-pattern: one key for all = one overloaded partition
- **Increasing partitions** changes routing of existing keys — plan upfront
- **`record.key()`** in consumer — for debugging routing decisions

<br>

> Key selection is a **business decision**: it determines correctness of your business logic.

---

<!-- _class: title -->

## What's Next — Branch 04: Consumer Groups

- 3 instances of `notification-service` in one consumer group
- Each instance reads **1 partition** from `03.orders.created`
- Stop one instance → **rebalance** → partitions redistributed automatically
- `RangeAssignor` vs `RoundRobinAssignor`

<br>

# Questions?

**Branch 03 — Message Keys**

`hash(key) % numPartitions` · `userId` · `StickyPartitioner`

*Apache Kafka for Certification & Production*