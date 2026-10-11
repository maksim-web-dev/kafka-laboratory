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
