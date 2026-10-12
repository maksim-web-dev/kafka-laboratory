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

# Branch 01
# Basic Kafka in Docker

**Apache Kafka for Certification & Production**

---

## Agenda

1. What is Apache Kafka?
2. Architecture — Order → Kafka → Notification
3. KRaft mode — Kafka without Zookeeper
4. Producer — `KafkaTemplate.send()`
5. Topic — the channel
6. Consumer — `@KafkaListener`
7. Consumer Group & Offset
8. JSON Serialization — two separate models
9. Summary & What's next

---

## What is Apache Kafka?

**A distributed event streaming platform**

- Messages are written to a **topic** (an ordered, persistent log)
- Any number of **consumers** can read from the same topic independently
- Messages are **not deleted** after reading — they live until retention expires
- Designed for **high throughput** and **fault tolerance**

<br>

> Kafka is not a message queue — it is a **distributed commit log**.
> Consumers read at their own pace and never block each other.

---

## Architecture — Branch 01

```
POST /api/orders
      │
      ▼
┌─────────────────┐   topic: 01.orders.created   ┌──────────────────────────┐
│  order-service  │  ─────────────────────────▶  │  notification-service    │
│  :8081          │        (JSON message)         │  :8082                   │
└─────────────────┘                               └──────────────────────────┘
         │                                                    │
         └────────────────────┬───────────────────────────────┘
                              │
                   ┌──────────▼──────────┐
                   │    Apache Kafka     │
                   │    (KRaft mode)     │
                   │       :9092        │
                   └──────────┬──────────┘
                              │
                   ┌──────────▼──────────┐
                   │      Kafka UI       │
                   │       :8080        │
                   └─────────────────────┘
```

---

## KRaft Mode — Kafka Without Zookeeper

**Apache Kafka 3.3+ replaces Zookeeper with its own Raft consensus**

```
Old setup (pre-3.3):          New setup (KRaft):
┌───────────┐                 ┌───────────────────────┐
│ Zookeeper │ ← extra process │  Kafka (broker +      │
│  :2181    │                 │   controller in one)  │
└───────────┘                 │   PROCESS_ROLES=      │
┌───────────┐                 │   broker,controller   │
│  Kafka    │                 └───────────────────────┘
│  :9092    │
└───────────┘
```

- **Simpler** — one process instead of two
- **Faster** startup and shutdown
- **Required** from Kafka 4.0 — Zookeeper is completely removed

> From Kafka 4.0 KRaft is the **only** option.
> Learn it now — it is what production clusters use.

---

## Topic — The Channel

**A topic is a named, ordered, persistent log of messages**

```
topic: 01.orders.created   (1 partition, retention: 7 days)

partition 0:
  [offset=0] OrderCreatedEvent { orderId=..., userId="user-42", ... }
  [offset=1] OrderCreatedEvent { orderId=..., userId="user-99", ... }
  [offset=2] OrderCreatedEvent { ... }
```

- Created automatically via a Spring **`NewTopic`** bean on startup
- `1 partition` — sufficient for branch01; expanded in branch02
- `replication-factor=1` — single-broker development setup

> Topic naming convention: `<branch>.<domain>.<event-type>`
> Example: `01.orders.created`

---

## Producer — KafkaTemplate.send()

**Three steps from HTTP request to Kafka message:**

```
1. OrderController.createOrder(request)
        │
        ▼
2. OrderService → builds OrderCreatedEvent(
       orderId = UUID.randomUUID(),
       userId  = request.userId,
       ...
   )
        │
        ▼
3. kafkaTemplate.send(topic, key, event)
   key   = orderId   (prepares for partition routing in branch03)
   value = JSON bytes  (spring.json.add.type.headers=false)
```

- Callback logs **partition** and **offset** after broker acknowledgement
- `spring.json.add.type.headers=false` — simplest serialization for branch01

---

## Consumer — @KafkaListener

**Spring Boot subscribes to a topic with a single annotation:**

```kotlin
@KafkaListener(
    topics     = ["01.orders.created"],
    groupId    = "notification-service-group"
)
fun handleOrder(record: ConsumerRecord<String, OrderCreatedEvent>) {
    val event = record.value()
    println("Order ${event.orderId} from ${event.userId}")
    // simulate sending an email
}
```

- `auto-offset-reset: earliest` — on first start, reads all existing messages
- `ConsumerRecord<K, V>` gives access to **partition**, **offset**, and **headers**
- `JsonDeserializer` with `value.default.type` deserializes JSON → Kotlin class

---

## Consumer Group & Offset

**Consumer group = a logical set of consumers sharing the work**

```
topic: 01.orders.created (1 partition)

notification-service-group:
  consumer-1  reads  partition 0
              current offset = 3  (next message to read)
              log-end offset = 5
              lag = 2
```

- **Offset** is stored in Kafka's internal topic `__consumer_offsets`
- On restart → consumer continues from the **last committed offset**
- **Lag** = `log-end-offset − current-offset` → growing lag means the consumer is falling behind

> `auto-offset-reset: earliest` — if the group has no stored offset,
> start reading from the **beginning** of the topic.

---

## JSON Serialization — Two Separate Models

**Both services define `OrderCreatedEvent` independently**

```
order-service:
  com.kafkalab.order.model.OrderCreatedEvent
  → serialized to JSON by KafkaTemplate (JsonSerializer)

notification-service:
  com.kafkalab.notification.model.OrderCreatedEvent
  → deserialized from JSON by @KafkaListener (JsonDeserializer)
```

> Services are **independent** — they must not share a common library.
> The JSON contract (field names) is the only coupling.

<br>

- `spring.json.add.type.headers=false` — producer does NOT embed class name in headers
- `spring.json.value.default.type` — consumer always deserializes to the specified class
- From **branch11** (Avro + Schema Registry) this coupling is solved via schema contracts

---

## Key Takeaways

- **Topic** = ordered, persistent log; messages survive consumer restarts
- **Producer** publishes via `KafkaTemplate.send(topic, key, value)`
- **Consumer** subscribes with `@KafkaListener`; one annotation is enough
- **Consumer Group** = multiple instances sharing partitions; group tracks offsets
- **Offset** = message position; stored in `__consumer_offsets`; enables replay
- **KRaft** = Kafka without Zookeeper; required from Kafka 4.0
- **Two models** = services stay independent; JSON field names are the contract

<br>

> Branch01 is the minimal working Kafka setup.
> Every concept introduced here is the foundation for all following branches.

---

<!-- _class: title -->

## What's Next — Branch 02

**Topics & Partitions in depth**

- Multiple topics per event type (`orders.created`, `orders.cancelled`)
- Partitions — the unit of parallelism and ordering
- Retention configuration via `NewTopic` bean
- Topic naming conventions for production

<br>

# Questions?

**Branch 01 — Basic Kafka in Docker**

*Apache Kafka for Certification & Production*