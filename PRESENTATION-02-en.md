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

# Branch 02
# Topics & Partitions

**Apache Kafka for Certification & Production**

---

## Agenda

1. Why branch01 wasn't enough
2. What is a Partition?
3. How partitions are selected
4. Offsets — Kafka's bookmark system
5. Retention — how long messages live
6. 1 partition = strict ordering
7. Naming conventions
8. Type Headers — multi-type deserialization
9. Summary & What's next

---

## Branch01 — One Partition, One Stream

**The limitation:**

```
1 topic → 1 partition → 1 consumer → sequential processing
```

- Max throughput = the speed of a **single consumer**
- One message type — one listener
- No isolation between event types (created vs cancelled)

<br>

**Branch02 introduces:**
- Multiple partitions for parallel processing
- Separate topics per event type
- Type headers for flexible deserialization

---

## What is a Partition?

**A partition is an ordered, immutable log inside a topic**

```
02.orders.created  (3 partitions)
├── partition 0:  [offset=0] [offset=3] [offset=6] ...
├── partition 1:  [offset=0] [offset=2] [offset=5] ...
└── partition 2:  [offset=0] [offset=1] [offset=4] ...
```

> Kafka guarantees ordering **only within** a single partition.
> Between partitions — order is undefined.

Each partition is assigned to **exactly one** consumer in a group.

---

## Partitions Enable Parallelism

**Scale-out by adding consumers (up to partition count):**

```
3 partitions + 1 consumer:      3 partitions + 3 consumers:
consumer-1 reads p0, p1, p2     consumer-1 reads p0
                                consumer-2 reads p1
                                consumer-3 reads p2
```

> **Rule:** you cannot have more useful consumers than partitions.
> Extra consumers sit **idle**.

---

## Partition Selection

**Rule: `partition = hash(key) % numPartitions`**

```
key = "order-uuid-123"  →  hash % 3 = 0  →  partition 0
key = "order-uuid-456"  →  hash % 3 = 2  →  partition 2
key = null              →  round-robin   →  0, 1, 2, 0, 1, 2 ...
```

<br>

| Key | Partition selection | Ordering guarantee |
|-----|--------------------|--------------------|
| Present | `murmur2(key) % N` | All events with same key are ordered |
| `null` | round-robin / sticky | No ordering guarantee |

---

## Offset — Kafka's Bookmark

**Offset = monotonically increasing message number within a partition**

```
partition 0:  [0] [1] [2] [3] [4] [5] ...
                           ↑
                  consumer offset = 3
                  (next message to read: [3])
```

- Stored in internal topic **`__consumer_offsets`**
- On restart → consumer continues from the same position
- Can be **reset** to replay events (`--reset-offsets`)

> **Consumer lag** = `LOG-END-OFFSET - CURRENT-OFFSET`
> Growing lag → consumer is falling behind → alert!

---

## Retention — Message Lifetime

**`retention.ms` = how long a message lives after being written**

```
02.orders.created     → 7 days   (604_800_000 ms)
02.notifications.sent → 1 day    ( 86_400_000 ms)
```

<br>

**Key facts:**
- Messages are deleted **after the time expires**, regardless of whether they were consumed
- If a consumer is behind by more than the retention period → **messages are lost**
- Also: `retention.bytes` — size-based limit per partition

> Kafka is a **log**, not a traditional message queue.
> Messages survive consumer crashes — within the retention window.

---

## 1 Partition = Strict Ordering

**When order is critical — use exactly 1 partition**

```
02.orders.cancelled  (1 partition)
→ all cancellations go to partition 0
→ processed strictly in arrival order: cancel#1 before cancel#2
```

<br>

**Why not use more partitions?**

With 2 partitions: `cancel#1 → p0`, `cancel#2 → p1`
Two consumers may process them in reverse order. 

**Trade-off:** 1 partition = 1 consumer = lower throughput.
Acceptable when strict ordering matters more than scale.

---

## Naming Conventions

**Topic format: `<env>.<domain>.<event-type>`**

```
02 . orders  . created
 │      │         │
 │      │         └── what happened: created, cancelled, processed
 │      └──────────── business domain: orders, payments, notifications
 └─────────────────── environment / branch prefix
```

<br>

**Examples:**
- Learning: `02.orders.created`
- Production: `prod.orders.created`
- Staging: `staging.payments.processed`

> Consistent naming = self-documenting topics = faster incident response.

---

## Type Headers — The Problem

**How does `JsonDeserializer` know which class to instantiate?**

**Option A:** `value.default.type = OrderCreatedEvent`
→ only **one** type for the entire factory ✗

**Option B:** Two separate `KafkaListenerContainerFactory`
→ manual bean configuration, `@KafkaListener(containerFactory = "...")` binding ✗

**Option C:** Deserialize to `Map<String, Any>`
→ loss of type safety, manual parsing ✗

<br>

> None of these scale well beyond 3–4 event types.

---

## Type Headers — The Solution

**`spring.json.add.type.headers=true`**

```
Producer sends:
  { JSON bytes }  +  header: __TypeId__ = "OrderCreatedEvent"

Consumer JsonDeserializer:
  reads __TypeId__
  → looks in type.mapping
  → "OrderCreatedEvent" → com.kafkalab.notification.model.OrderCreatedEvent
  → instantiates the correct class automatically ✓
```

<br>

**One factory — any number of message types.**

Aliases (`OrderCreatedEvent` instead of FQCN) decouple services from package names.

---

## Type Headers — Why Aliases Matter

**Without aliases:**
```yaml
__TypeId__: com.kafkalab.order.model.OrderCreatedEvent
```
Consumer must know the **exact package** of the producer.
Rename a package → update every consumer.

**With aliases:**
```yaml
producer type.mapping: "OrderCreatedEvent:com.kafkalab.order.model.OrderCreatedEvent"
consumer type.mapping: "OrderCreatedEvent:com.kafkalab.notification.model.OrderCreatedEvent"
```
Services share **the alias**, not the class path.
Each service maps it to its own local class independently.

---

## Branch02 Topics Overview

| Topic | Partitions | Retention | Purpose |
|-------|-----------|-----------|---------|
| `02.orders.created` | **3** | 7 days | New orders — parallel processing |
| `02.orders.cancelled` | **1** | 7 days | Cancellations — strict order |
| `02.payments.processed` | **3** | 7 days | Reserved for branch03+ |
| `02.notifications.sent` | **1** | 1 day | Delivery confirmations |

> Prefix `02.` keeps this branch's topics isolated in the same Kafka cluster.

---

## Architecture — branch02

```
POST /api/orders
      │
      ▼
┌─────────────────┐   02.orders.created (3 partitions)   ┌──────────────────────────┐
│  order-service  │  ──────────────────────────────────▶ │                          │
│  :8081          │                                      │  notification-service    │
│                 │   02.orders.cancelled (1 partition)  │  :8082                   │
│                 │  ──────────────────────────────────▶ │                          │
└─────────────────┘                                      └──────────────────────────┘
                              │
                   ┌──────────▼──────────┐
                   │    Apache Kafka     │
                   │    kafka:9092       │
                   │    (KRaft mode)     │
                   └─────────────────────┘
```

---

## Key Takeaways

- **Partition** = unit of parallelism AND ordering in Kafka
- **`hash(key) % N`** determines partition; `null` key = round-robin
- **Offset** = consumer bookmark; stored in `__consumer_offsets`
- **`retention.ms`** = message TTL, independent of consumption
- **1 partition** = strict order; **N partitions** = parallelism
- **Type headers** = scalable multi-type deserialization from one factory
- **Naming convention:** `<env>.<domain>.<event-type>`

<br>

> Good Kafka design is mostly about these 7 things.

---

## What's Next — Branch 03

**Message Keys in depth**

- How `hash(key) % numPartitions` selects a partition
- Why all events of the same `userId` must go to the same partition
- `null` key → round-robin distribution
- Comparison: keyed by `userId` vs keyed by `orderId`

<br>

> Key selection is a **business decision**, not a technical detail.
> Wrong key = wrong ordering = wrong behavior.

---

<!-- _class: title -->

# Questions?

**Branch 02 — Topics & Partitions**

`02.orders.created` · `02.orders.cancelled`

*Apache Kafka for Certification & Production*