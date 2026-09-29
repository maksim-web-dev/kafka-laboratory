# Branch 02 — Topics & Partitions

---

## `branch02_topics_partitions` — what we learn

- Expanding to 4 topics with prefix `02.` (distinguishing topics between branches in one cluster)
- Topic `02.orders.created` with 3 partitions — parallel processing
- Topic `02.orders.cancelled` with 1 partition — strict ordering
- Configuring `retention.ms` (`604_800_000` — 7 days, `86_400_000` — 1 day)
- `spring.json.add.type.headers=true` — type headers `__TypeId__` in messages
- `spring.json.type.mapping` — aliases instead of FQCN in the header
- Two `@KafkaListener` for two different event types in one consumer group
- CLI commands: `kafka-topics --describe`, `kafka-consumer-groups --describe`
- Concepts: Partition, Partition selection (hash % N), Offset, Retention, Naming conventions

---

## What changed compared to branch01

- Number of topics: 4 instead of 1
- Number of partitions: 3 instead of 1
- Added cancellation topic (`02.orders.cancelled` (1 partition))
- Added Type headers + aliases
- Added cancellation endpoint (`POST /api/orders/{id}/cancel`)
- Extended counter API (`{"orders_created": N, "orders_cancelled": N, "total": N}`)

---

## Architecture

```
POST /api/orders
      │
      ▼
┌─────────────────┐   topic: 02.orders.created (3 partitions)    ┌──────────────────────────┐
│  order-service  │  ─────────────────────────────────────────▶  │                          │
│  :8081          │                                              │  notification-service    │
│                 │   topic: 02.orders.cancelled (1 partition)   │  :8082                   │
│                 │  ─────────────────────────────────────────▶  │                          │
└─────────────────┘                                              └──────────────────────────┘
         │                                                                  │
         └─────────────────────────┬────────────────────────────────────────┘
                                   │
                      ┌────────────▼────────────┐
                      │      Apache Kafka       │
                      │      kafka:9092         │
                      │      (KRaft mode)       │
                      └────────────┬────────────┘
                                   │
                      ┌────────────▼────────────┐
                      │      Kafka UI           │
                      │      :8080              │
                      └─────────────────────────┘
```

### Topics and their configuration

- **`02.orders.created`** — 3 partitions, retention 7 days — new orders, parallel processing
- **`02.orders.cancelled`** — 1 partition, retention 7 days — cancellations, strict ordering required
- **`02.payments.processed`** — 3 partitions, retention 7 days — reserved for branch03+
- **`02.notifications.sent`** — 1 partition, retention 1 day — delivery confirmations, short retention

> The `02.` prefix separates this branch's topics from others — when switching between branches in the same Kafka cluster, topics do not overlap.

---

## Key concepts of this branch

### Partition

A partition is an ordered, immutable sequence of records inside a topic.
Kafka splits a topic into N partitions and distributes them among consumers in a group.

```
02.orders.created
├── partition 0: msg[0], msg[1], msg[4], ...
├── partition 1: msg[2], msg[5], msg[8], ...
└── partition 2: msg[3], msg[6], msg[9], ...
```

**Why 3 partitions for `02.orders.created`?**
- In branch04 we will run 3 instances of notification-service — each will get 1 partition.
- Right now (1 consumer) it reads all 3 partitions by itself.

### Offset

Every message in a partition has a monotonically increasing offset (0, 1, 2…).
Kafka UI shows the offset in the "Offset" column. The consumer group stores its current offset
in the internal topic `__consumer_offsets`.

### Retention

`retention.ms` defines how long Kafka retains messages after they are written.
Once the time expires — messages are deleted regardless of whether they were consumed.

```
02.orders.created    → 7 * 24 * 60 * 60 * 1000 = 604_800_000 ms = 7 days
02.notifications.sent → 1 * 24 * 60 * 60 * 1000 = 86_400_000 ms = 1 day
```

### Type Headers (new in branch02)

In branch01 the producer sent plain JSON without metadata headers (`spring.json.add.type.headers=false`).
In branch02 type headers with aliases are enabled:

```
Producer header:   __TypeId__ = "OrderCancelledEvent"
Consumer mapping:  "OrderCancelledEvent" → com.kafkalab.notification.model.OrderCancelledEvent
```

This allows a single consumer to listen to two topics with different message types.

---

## How to run

```bash
docker compose -f docker-compose-02.yml up --build
```

Verify readiness:

```bash
docker compose -f docker-compose-02.yml ps
# kafka, kafka-ui, order-service, notification-service — Running/healthy
```

---

## How to test

### 1. Create several orders (partition distribution)

```bash
for i in 1 2 3 4 5 6; do
  curl -s -X POST http://localhost:8081/api/orders \
    -H "Content-Type: application/json" \
    -d "{\"userId\":\"user-$i\",\"product\":\"Book $i\",\"quantity\":1,\"totalAmount\":$((i*10)).00}" | jq .orderId
done
```
or on Windows OS
```cmd
for i in 1 2 3 4 5 6; do
  curl -s -X POST http://localhost:8081/api/orders ^
    -H "Content-Type: application/json" ^
    -d "{\"userId\":\"user-$i\",\"product\":\"Book $i\",\"quantity\":1,\"totalAmount\":$((i*10)).00}"
done
```
Expected result in order-service logs:

```
OrderCreated published → topic=02.orders.created, partition=0, offset=0, key=<uuid>
OrderCreated published → topic=02.orders.created, partition=2, offset=0, key=<uuid>
OrderCreated published → topic=02.orders.created, partition=1, offset=0, key=<uuid>
```

> Partition is chosen by hashing the key (`orderId`). Results will be distributed across 0, 1, 2 — not necessarily in order.

### 2. Cancel an order

```bash
# substitute a real orderId from the previous response
ORDER_ID="ee5a4723-145f-4410-8dd9-72c9e83f1e86"

curl -s -X POST "http://localhost:8081/api/orders/${ORDER_ID}/cancel" \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-1","reason":"Changed my mind"}' | jq
```
or on Windows OS
```cmd
ORDER_ID="ee5a4723-145f-4410-8dd9-72c9e83f1e86"

curl -s -X POST "http://localhost:8081/api/orders/${ORDER_ID}/cancel" ^
  -H "Content-Type: application/json" ^
  -d '{"userId":"user-1","reason":"Changed my mind"}'
```

Response:
```json
{
  "orderId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "userId": "user-1",
  "reason": "Changed my mind",
  "timestamp": "2026-06-11T12:00:00"
}
```

order-service log:
```
OrderCancelled published → topic=02.orders.cancelled, partition=0, offset=0, key=f47ac10b-...
```

> `02.orders.cancelled` has 1 partition — partition=0 always.

### 3. Check counters

```bash
curl http://localhost:8082/api/notifications/count
```

```json
{
  "orders_created": 6,
  "orders_cancelled": 1,
  "total": 7
}
```

### 4. Browse topics in Kafka UI

Open in browser: [http://localhost:8080](http://localhost:8080)

- **Topics** → you will see `02.*` topics
- `02.orders.created` → **Partitions** tab: 3 partitions, each with its own offsets
- `02.orders.cancelled` → 1 partition
- **Messages** → JSON visible + `__TypeId__` header

### 5. CLI commands inside the container

```bash
# List all topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Detailed description (partitions, replication, leader)
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic 02.orders.created

# Check retention
docker exec kafka kafka-configs --bootstrap-server localhost:9092 \
  --describe --entity-type topics --entity-name 02.orders.created

# Consumer group status (offset, lag, partition assignment)
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group notification-service-group
```

Expected output of `--describe --topic 02.orders.created`:

```
Topic: 02.orders.created   PartitionCount: 3   ReplicationFactor: 1
  Topic: 02.orders.created  Partition: 0  Leader: 1  Replicas: 1  Isr: 1
  Topic: 02.orders.created  Partition: 1  Leader: 1  Replicas: 1  Isr: 1
  Topic: 02.orders.created  Partition: 2  Leader: 1  Replicas: 1  Isr: 1
```

---

## How it works internally

### Producer (order-service) — what changed

#### KafkaTopicConfig.kt

In branch01 there was one topic `01.orders.created` with 1 partition. Now there are 4 topics with explicit settings:

```kotlin
// 3 partitions + retention 7 days
TopicBuilder.name("02.orders.created")
    .partitions(3)
    .replicas(1)
    .config(TopicConfig.RETENTION_MS_CONFIG, "604800000")
    .build()
```

Topics `02.payments.processed` and `02.notifications.sent` are not actively used yet —
they are already present in the cluster and ready for branch03+.

#### OrderService.kt — type headers

In branch01: `spring.json.add.type.headers=false` — consumer knew the type via explicit configuration.
In branch02: `spring.json.add.type.headers=true` — every message carries the `__TypeId__` header.

Aliases (type mapping) in producer:

```yaml
spring.json.type.mapping: "OrderCreatedEvent:com.kafkalab.order.model.OrderCreatedEvent,
                            OrderCancelledEvent:com.kafkalab.order.model.OrderCancelledEvent"
```

Instead of `com.kafkalab.order.model.OrderCreatedEvent` in the header, the short alias `OrderCreatedEvent` is used.

#### OrderController.kt — new endpoint

```
POST /api/orders/{orderId}/cancel
Body: { "userId": "...", "reason": "..." }
→ publishes OrderCancelledEvent to 02.orders.cancelled
```

### Consumer (notification-service) — what changed

#### application.yml

In branch01 the consumer deserialized `OrderCreatedEvent` by default (`value.default.type`).
In branch02 the type is determined from the header + local mapping:

```yaml
spring.json.type.mapping: "OrderCreatedEvent:com.kafkalab.notification.model.OrderCreatedEvent,
                            OrderCancelledEvent:com.kafkalab.notification.model.OrderCancelledEvent"
```

#### OrderEventListener.kt — two listeners

```kotlin
@KafkaListener(topics = ["02.orders.created"])   // reads from 3 partitions
fun handleOrderCreated(record: ConsumerRecord<String, OrderCreatedEvent>)

@KafkaListener(topics = ["02.orders.cancelled"]) // reads from 1 partition
fun handleOrderCancelled(record: ConsumerRecord<String, OrderCancelledEvent>)
```

Both listeners belong to the same `notification-service-group`.
Kafka treats them as a single consumer in the group.

---

## Project structure (changes relative to branch01)

```
kafka-laboratory/
├── docker-compose-02.yml                  ← ports 8081/8082 (same as branch01)
├── branch02_topics_partitions/
│   ├── order-service/
│   │   └── src/main/kotlin/com/kafkalab/order/
│   │       ├── config/KafkaTopicConfig.kt    ← 4 topics with prefix 02. (with retention)
│   │       ├── controller/OrderController.kt ← new POST /{id}/cancel
│   │       ├── model/
│   │       │   ├── CancelOrderRequest.kt     ← NEW
│   │       │   └── OrderCancelledEvent.kt    ← NEW
│   │       └── service/OrderService.kt       ← added cancelOrder(), KafkaTemplate<String, Any>
│   │   └── src/main/resources/
│   │       └── application.yml              ← port 8081, type.headers=true, type.mapping
│   └── notification-service/
│       └── src/main/kotlin/com/kafkalab/notification/
│           ├── controller/NotificationController.kt ← split by topics
│           ├── listener/OrderEventListener.kt       ← two @KafkaListener
│           └── model/
│               └── OrderCancelledEvent.kt           ← NEW
│       └── src/main/resources/
│           └── application.yml                      ← port 8082, type.mapping
```

---

## Key concepts of this branch

- **Partition** — `02.orders.created` has 3 partitions, visible in logs and Kafka UI
- **Partition selection** — Key (orderId) → hash % 3 → different partition for different orders
- **Offset** — each partition has its own offset counter, starting from 0
- **Retention** — `02.orders.created` 7 days, `02.notifications.sent` 1 day
- **Naming conventions** — `<branch>.<domain>.<event-type>`: `02.orders.created`, `02.payments.processed`
- **Type headers** — `__TypeId__: OrderCancelledEvent`, consumer determines class from the header
- **Multiple topics** — one consumer group reads from two topics simultaneously
- **1 partition = strict order** — `02.orders.cancelled`, 1 partition guarantees ordering of cancellations

---

## What's next — branch03

In the next branch we study Message Keys in depth:
- How `hash(key) % numPartitions` selects a partition
- Why all events of the same `userId` must go to the same partition
- `null` key → round-robin distribution
- Comparison: with key (`userId`) vs without key (`orderId`)

------------------------------------------

## Deep dive: why Type Headers if you can just use two @KafkaListener?

### Is this implemented in branch02?

Yes. `OrderEventListener` has two methods with different types:

```kotlin
@KafkaListener(topics = ["02.orders.created"], groupId = "notification-service-group")
fun handleOrderCreated(record: ConsumerRecord<String, OrderCreatedEvent>)   // type A

@KafkaListener(topics = ["02.orders.cancelled"], groupId = "notification-service-group")
fun handleOrderCancelled(record: ConsumerRecord<String, OrderCancelledEvent>) // type B
```

And the `application.yml` of notification-service **does not have** `spring.json.value.default.type`.
This means `JsonDeserializer` relies entirely on the `__TypeId__` header of each message
to determine which class to instantiate.

### Why isn't "same groupId + two methods" enough?

The issue is not the groupId or the number of methods — it's about **how `JsonDeserializer` determines the type**.

The method signature (`ConsumerRecord<String, OrderCreatedEvent>`) is compile-time information.
`JsonDeserializer` is a separate component that runs **before** the message reaches
the method. It cannot see the signature — it only sees bytes and configuration.

Without type headers there are three options:

- **`value.default.type`** — one type for all messages in the factory; problem: cannot have two different types in one factory
- **Two separate `KafkaListenerContainerFactory`** — each factory has its own `JsonDeserializer` with a specific type; problem: manual configuration of two beans + binding via `@KafkaListener(containerFactory = "...")`
- **Deserialize to `Map<String, Any>` or `String`** — no type needed; problem: loss of type safety, manual parsing

With `spring.json.add.type.headers=true` the producer adds `__TypeId__` to every message.
`JsonDeserializer` reads this header and selects the class itself — **one factory, any number of types**.

### Comparison diagram

```
WITHOUT type headers (branch01-style):
───────────────────────────────────────
Producer: { JSON bytes }                 ← no metadata
Consumer JsonDeserializer: "what type?"  ← looks in configuration
  → value.default.type = OrderCreatedEvent  ← one for all, or
  → separate factory per type            ← manual configuration

WITH type headers (branch02):
──────────────────────────────
Producer: { JSON bytes } + header(__TypeId__ = "OrderCreatedEvent")
Consumer JsonDeserializer: "what type?"
  → reads __TypeId__ = "OrderCreatedEvent"
  → looks in type.mapping → com.kafkalab.notification.model.OrderCreatedEvent
  → instantiates the correct class ✓ (automatically, no extra factories)
```

### When type headers are especially important

In branch02 each topic contains **one** event type, so two factories would work.
But imagine a topic `domain.events` where `UserRegistered`, `UserUpdated`,
and `UserDeleted` all flow through the same queue — without type headers you need a separate
factory for each of the three types, or deserialization to a generic type with a manual switch.
With type headers — one factory, and each message announces itself as "I am UserRegistered".

---
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------

## Presentation Slides — Branch 02: Topics & Partitions

---

### Slide 1 — Title

**Branch 02: Topics & Partitions**

- Partitions and parallel processing
- Offsets and retention
- Type headers for multi-type consumers
- Naming conventions in production systems

---

### Slide 2 — What branch01 had and why it wasn't enough

**Branch01: one partition — one stream**

```
1 topic → 1 partition → 1 consumer → sequential processing
```

- Maximum throughput = the speed of one consumer
- One message type — one listener
- No isolation between event types (created vs cancelled)

**Branch02 solution:** multiple partitions + separate topics for different event types

---

### Slide 3 — What is a partition

**Partition = an ordered queue inside a topic**

```
02.orders.created  (3 partitions)
├── partition 0:  msg[offset=0], msg[offset=3], msg[offset=6] ...
├── partition 1:  msg[offset=0], msg[offset=2], msg[offset=5] ...
└── partition 2:  msg[offset=0], msg[offset=1], msg[offset=4] ...
```

- Kafka guarantees order **only within** a single partition
- Between partitions — order is undefined
- Each partition is read by exactly one consumer in the group

---

### Slide 4 — How a partition is selected

**Rule: `partition = hash(key) % numPartitions`**

```
key = "order-uuid-123"  →  hash = 2847162  →  2847162 % 3 = 0  →  partition 0
key = "order-uuid-456"  →  hash = 9183714  →  9183714 % 3 = 2  →  partition 2
key = null              →  round-robin  →  0, 1, 2, 0, 1, 2 ...
```

- The same key **always** goes to the same partition
- This guarantees ordering for all events of one entity (order, user)
- `null` key → even distribution, but no ordering guarantee

---

### Slide 5 — Offset

**Offset = monotonic message number within a partition**

```
partition 0:  [0] [1] [2] [3] [4] ...
                           ↑
                    consumer offset = 3 (next to read — [3])
```

- Consumer group stores its offset in the `__consumer_offsets` topic
- On restart — resumes from the same position
- Can rewind offset (`--reset-offsets`) for reprocessing
- Kafka UI shows: `LOG-END-OFFSET`, `CURRENT-OFFSET`, `LAG`

---

### Slide 6 — Retention: how long messages live

**`retention.ms` = storage time after write**

```
02.orders.created    → 7 days  (604_800_000 ms)
02.notifications.sent → 1 day  (86_400_000 ms)
```

- Message is deleted **after time expires**, regardless of whether it was consumed
- If a consumer "falls behind" by more than retention — it will lose messages
- Also exists: `retention.bytes` — partition size limit

---

### Slide 7 — 1 partition = strict order

**When order is critical — use 1 partition**

```
02.orders.cancelled  (1 partition)
→ partition 0 always
→ cancellations processed strictly in arrival order
```

- Cancellation #1 is always processed before cancellation #2
- With more than 1 partition — two cancellations for the same order may arrive in different order
- Trade-off: throughput is limited to one consumer

---

### Slide 8 — Naming conventions

**Topic format: `<prefix>.<domain>.<event-type>`**

```
02.orders.created
02.orders.cancelled
02.payments.processed
02.notifications.sent
│    │         │
│    │         └── event type (created, cancelled, processed)
│    └────────────── domain (orders, payments, notifications)
└─────────────────── branch / environment prefix
```

In production, instead of a branch number, use the environment: `prod.`, `staging.`, `dev.`

---

### Slide 9 — Type Headers: the problem

**How does JsonDeserializer determine the type without headers?**

```
Option A: value.default.type = OrderCreatedEvent
           → only one type for the entire factory

Option B: two separate KafkaListenerContainerFactory
           → manual configuration, binding via containerFactory="..."

Option C: deserialize to Map<String,Any>
           → loss of type safety
```

None of these options scales well with 5+ event types in one service.

---

### Slide 10 — Type Headers: the solution

**`spring.json.add.type.headers=true` → `__TypeId__` in the header**

```
Producer sends:
  { JSON bytes }  +  header: __TypeId__ = "OrderCreatedEvent"

Consumer JsonDeserializer:
  reads __TypeId__  →  looks in type.mapping
  "OrderCreatedEvent" → com.kafkalab.notification.model.OrderCreatedEvent
  → instantiates the correct class automatically
```

**One factory — any number of message types.**

Aliases (`OrderCreatedEvent` instead of FQCN) remove the package name dependency between services.

---

### Slide 11 — Summary of branch02

**What we learned:**

- Partition — the unit of parallelism and ordering in Kafka
- `hash(key) % N` determines the partition; `null` key — round-robin
- Offset — consumer position; stored in `__consumer_offsets`
- `retention.ms` — message lifetime, independent of consumption
- 1 partition = strict order; N partitions = parallelism
- Type headers — scalable approach to deserializing multiple types
- Naming convention: `<env>.<domain>.<event-type>`

**Next — branch03:** Message Keys in depth: why `userId` is better than `orderId` as a key.

---

---------------------------------------------------------------------------------------------------------

## Presentation Script — Branch 02: Topics & Partitions

### Introduction (Slide 1)

In the first branch we launched the simplest possible Kafka system: one topic, one partition, one producer, one consumer.
That's great for getting started, but it doesn't reflect real-world systems.
In branch02 we take a step toward production patterns: we'll understand partitions, offsets, retention, and learn how to name topics properly.

---

### The single-partition problem (Slide 2)

Imagine an online store on Black Friday. Thousands of orders per minute.
If you have one topic with one partition — all processing is sequential. One consumer reads one message at a time.
You can't scale: a second consumer in the same group would just sit idle — nothing to read.
Partitions are the solution. Three partitions = three consumers can read in parallel.

---

### What is a partition (Slide 3)

A topic is a logical container. Physically it is made of partitions.
Each partition is an ordered, immutable sequence of messages. Like a log file.
Important: Kafka guarantees order only within a single partition. Between partitions there is no ordering.
That's why choosing the number of partitions and the message key is an architectural decision.

---

### How a partition is selected (Slide 4)

When a producer sends a message with a key, Kafka computes: `partition = hash(key) % numPartitions`.
This is deterministic: the same key always goes to the same partition.
In our example the key is `orderId`. So all events of one order are guaranteed to land in one partition — and will be processed in the correct order.
If there's no key (`null`) — round-robin: messages are distributed evenly across partitions, but there's no ordering between them.

---

### Offset (Slide 5)

Every message in a partition gets a monotonically increasing number — the offset. 0, 1, 2, 3...
The consumer group remembers which message it has already processed — it stores the "current offset" in the special internal topic `__consumer_offsets`.
If a consumer crashes and restarts — it reads the offset from `__consumer_offsets` and continues from where it left off.
This is Kafka's reliability model: at-least-once delivery by default.
The difference between the current offset and the end of the partition is the "lag". If lag grows — the consumer is falling behind.

---

### Retention (Slide 6)

Kafka is not a queue that deletes messages immediately after they're consumed. It's a log.
`retention.ms` defines how long a message lives. After that — it's deleted forever.
For orders we keep 7 days: we can replay events, debug the system, recover after a consumer failure.
For notification confirmations — 1 day is enough: if it wasn't processed within a day, it's no longer relevant.
Note: deletion happens regardless of whether the message was consumed. If a consumer fell behind by more than the retention period — it will miss those messages.

---

### 1 partition and strict ordering (Slide 7)

The topic `02.orders.cancelled` has exactly one partition. Why?
The ordering of cancellations is critical. If a user cancelled an order twice (retry) — we must process the first cancellation before the second.
With one partition this is guaranteed: all messages are read strictly sequentially.
With three partitions — two cancellations for the same order could land in different partitions and be processed out of order.
Trade-off: one partition = one consumer = lower throughput. But for cancellations this is acceptable.

---

### Naming conventions (Slide 8)

Proper topic naming is the foundation of a maintainable system.
Our format: `<prefix>.<domain>.<event-type>`.
The `02.` prefix is the branch number in our learning context. In production this will be `prod.`, `staging.`, or the team name.
Domain — the business area: `orders`, `payments`, `notifications`.
Event type — what happened: `created`, `cancelled`, `processed`, `sent`.
Looking at the topic name `prod.orders.created` — immediately clear: production, orders, creation event.

---

### Type Headers (Slides 9–10)

In branch01 we had one topic and one message type — `OrderCreatedEvent`. The consumer knew the type in advance via `value.default.type`.
In branch02 we have two topics and two types: `OrderCreatedEvent` and `OrderCancelledEvent`. How will the deserializer know which class to create?
The "two separate factories" option works, but doesn't scale. 10 types = 10 factories, 10 `@KafkaListener(containerFactory="...")`.
The solution: `spring.json.add.type.headers=true`. The producer adds a `__TypeId__` header with the type alias to every message.
The consumer reads this header, finds the corresponding class in `type.mapping`, and instantiates it automatically.
Aliases — so we don't bind to a specific package. If we rename a package — we only update the mapping, not the producer.

---

### Summary (Slide 11)

Today we covered the fundamental concepts that every Kafka project relies on.
Partitions are not just "more throughput". They're an architectural decision about order and parallelism.
Offsets give us reliability: we can re-read, recover, and replay events.
Retention is a conscious decision: how long a business event should remain available.
Type headers are a production-ready approach to deserializing multi-type messages.
In the next branch we go deeper into Message Keys: why the choice of key is a business decision, not a technical detail.

---

---------------------------------------------------------------------------------------------------------

## Quiz — Branch 02: Topics & Partitions

---

**Question 1.**
What is a partition in Apache Kafka?

- A) A separate Kafka broker in the cluster
- B) An ordered sequence of messages inside a topic
- C) A consumer group reading from one topic
- D) A network segment between producer and consumer

**Correct answer: B**
A partition is an ordered, immutable sequence (log) of messages. A topic is made up of one or more partitions.

---

**Question 2.**
Kafka guarantees message ordering:

- A) Across all partitions of a topic
- B) Only within a single partition
- C) Within a single consumer group
- D) Across all topics on one broker

**Correct answer: B**
Ordering is guaranteed only within a partition. Between different partitions, ordering is undefined.

---

**Question 3.**
A producer sends a message with key `"user-42"` to a topic with 4 partitions. How is the partition selected?

- A) Randomly
- B) Round-robin across all partitions
- C) `hash("user-42") % 4`
- D) Always partition 0

**Correct answer: C**
With a key present, Kafka uses the DefaultPartitioner: `murmur2_hash(key) % numPartitions`. The same key always goes to the same partition.

---

**Question 4.**
A producer sends a message without a key (`key = null`). What is the default distribution strategy?

- A) Always partition 0
- B) Hash of the message value
- C) Round-robin or sticky partitioning
- D) Random partition, new for each message

**Correct answer: C**
With a `null` key, Kafka uses round-robin or (since Kafka 2.4) sticky partitioning — accumulates messages in one partition until the batch is full, then switches to another.

---

**Question 5.**
What is an offset in Kafka?

- A) The delay between producer and consumer in milliseconds
- B) A monotonically increasing message number within a partition
- C) The position of a broker in the cluster
- D) The total number of messages in a topic

**Correct answer: B**
An offset is the unique sequential number of a message within a specific partition. Starts at 0, increases monotonically.

---

**Question 6.**
Where does Kafka store the current offset of a consumer group?

- A) In ZooKeeper
- B) In the broker's file system
- C) In the internal topic `__consumer_offsets`
- D) In the consumer's memory

**Correct answer: C**
Starting with Kafka 0.9, offsets are stored in the internal topic `__consumer_offsets`, not in ZooKeeper.

---

**Question 7.**
What happens to a message after `retention.ms` expires?

- A) It is moved to an archive
- B) It is marked as "read" and kept
- C) It is deleted regardless of whether it was consumed
- D) It is moved to the next partition

**Correct answer: C**
Retention is the TTL for messages. Kafka deletes them after the time expires, even if a consumer hasn't read them yet.

---

**Question 8.**
How many consumers from one consumer group can read a single partition simultaneously?

- A) Unlimited
- B) Exactly 2 for fault tolerance
- C) Exactly 1
- D) Depends on `max.poll.records`

**Correct answer: C**
Kafka guarantees that exactly one consumer within a consumer group reads a given partition. This is the foundation of Kafka's parallelism model.

---

**Question 9.**
A consumer group has 2 consumers, the topic has 5 partitions. How will partitions be distributed?

- A) Each consumer reads all 5 partitions
- B) One gets 3 partitions, the other gets 2
- C) One gets all 5, the other is idle
- D) Kafka returns an error — number of consumers must equal number of partitions

**Correct answer: B**
Kafka distributes partitions evenly: with 5 partitions and 2 consumers the split will be 3+2 or 2+3.

---

**Question 10.**
A consumer group has 4 consumers, but the topic has only 3 partitions. What happens to the fourth consumer?

- A) It reads all partitions as a standby
- B) It remains idle (receives no partitions)
- C) Kafka automatically increases the partition count to 4
- D) It reads the same partition as the third consumer

**Correct answer: B**
You cannot usefully have more consumers than partitions. Extra consumers remain idle. That's why partition count defines the maximum degree of parallelism.

---

**Question 11.**
Why was 1 partition chosen for the topic `02.orders.cancelled`?

- A) To save disk space
- B) To guarantee strict ordering of cancellation processing
- C) Because Kafka doesn't support more than 1 partition for this type of topic
- D) To simplify consumer configuration

**Correct answer: B**
With one partition Kafka guarantees that all cancellations are processed strictly in arrival order. With multiple partitions, two cancellations for the same order could be processed out of order.

---

**Question 12.**
What is "consumer lag"?

- A) The delay between producer and Kafka broker
- B) The difference between the consumer's current offset and the last offset in the partition
- C) The number of messages sent by the producer per second
- D) The time for a consumer to reconnect after a failure

**Correct answer: B**
Lag = `LOG-END-OFFSET - CURRENT-OFFSET`. Shows how many messages the consumer is behind real-time data. Growing lag is an alert signal.

---

**Question 13.**
What does the setting `spring.json.add.type.headers=true` do?

- A) Adds HTTP headers to Kafka messages
- B) Forces the consumer to check the type before deserialization
- C) The producer adds a `__TypeId__` header with the Java class name to every message
- D) Enables message encryption by type

**Correct answer: C**
When `true`, Spring Kafka producer adds a `__TypeId__` header to every message, whose value is the fully-qualified class name or alias from `type.mapping`.

---

**Question 14.**
Why is `spring.json.type.mapping` needed ("OrderCreatedEvent:com.kafkalab...OrderCreatedEvent")?

- A) To reduce message size
- B) To decouple the class name in the header from a specific package
- C) To enable message compression
- D) To configure type processing priority

**Correct answer: B**
Aliases allow producer and consumer to have different package structures. The header stores the short alias `OrderCreatedEvent`, not `com.kafkalab.order.model.OrderCreatedEvent` — if the package changes, only the mapping needs updating.

---

**Question 15.**
A consumer group has one `KafkaListenerContainerFactory` and two `@KafkaListener` for different topics with different event types. Without type headers, what becomes a problem?

- A) Two listeners cannot belong to the same consumer group
- B) `JsonDeserializer` cannot determine the type without an external hint and will use `value.default.type` — one for all messages
- C) Kafka won't allow connecting to two topics simultaneously
- D) The consumer will get a `ClassCastException` on every other message

**Correct answer: B**
`JsonDeserializer` doesn't see the method signature — it sees bytes. Without `__TypeId__` in the header it can only use `value.default.type`, which gives one fixed type for all messages.

---

**Question 16.**
What format is recommended for naming topics in a production system?

- A) Random UUIDs to avoid conflicts
- B) `<env>.<domain>.<event-type>`, e.g. `prod.orders.created`
- C) Just the microservice name, e.g. `order-service`
- D) API version number, e.g. `v1-orders`

**Correct answer: B**
The widely accepted pattern: environment, domain, event type. Allows having `prod.orders.created` and `staging.orders.created` in the same cluster without conflicts.

---

**Question 17.**
Retention is set to 7 days. A consumer didn't read the topic for 10 days. What happens after it recovers?

- A) The consumer reads all 10 days of messages
- B) The consumer gets an error and cannot connect
- C) Messages from the first 3 days are lost; the consumer starts from the oldest available message
- D) Kafka automatically extends retention to 10 days

**Correct answer: C**
Messages older than `retention.ms` are deleted. The consumer will receive an `OffsetOutOfRangeException` or be moved to the `earliest` available offset — depending on `auto.offset.reset`.

---

**Question 18.**
What CLI command should you use to check the lag of consumer group `notification-service-group`?

- A) `kafka-topics --describe --group notification-service-group`
- B) `kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group notification-service-group`
- C) `kafka-offsets --list --group notification-service-group`
- D) `kafka-consumer-groups --lag --group notification-service-group`

**Correct answer: B**
`kafka-consumer-groups --describe` shows for each partition: `CURRENT-OFFSET`, `LOG-END-OFFSET`, `LAG`, `CONSUMER-ID`, `HOST`.

---

**Question 19.**
What does the command `kafka-topics --describe --topic 02.orders.created` show?

- A) A list of all messages in the topic
- B) Partition count, replication factor, leader broker for each partition, and topic configs
- C) Current offsets of all consumer groups
- D) Throughput statistics for the last 24 hours

**Correct answer: B**
`--describe` outputs topic metadata: `PartitionCount`, `ReplicationFactor` and for each partition — `Leader`, `Replicas`, `Isr`, and overridden configs (e.g. `retention.ms`).

---

**Question 20.**
A topic `domain.events` has `UserRegistered`, `UserUpdated`, and `UserDeleted` events all flowing through the same queue. Which deserialization approach is most scalable?

- A) Three separate `KafkaListenerContainerFactory` — one per type
- B) Deserialize to `String`, then manual `ObjectMapper.readValue()` with a `type` field check
- C) `spring.json.add.type.headers=true` with `type.mapping` — one factory, type determined from `__TypeId__` header
- D) One `@KafkaListener` with a `Map<String, Any>` parameter and a switch on key

**Correct answer: C**
Type headers are the cleanest approach: one factory, automatic deserialization to the right class, no need to change the consumer when adding a new type — just add a new line to `type.mapping`.