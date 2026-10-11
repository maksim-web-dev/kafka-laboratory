# Branch 01 — Basic Kafka in Docker

## What's implemented

A minimal but complete system of two Spring Boot services communicating via Apache Kafka.

```
POST /api/orders
      │
      ▼
┌─────────────────┐      topic: 01.orders.created        ┌──────────────────────────┐
│  order-service  │  ──────────────────────────────────▶ │  notification-service    │
│  :8081          │         (JSON message)               │  :8082                   │
└─────────────────┘                                      └──────────────────────────┘
         │                                                          │
         └─────────────────────┬────────────────────────────────────┘
                               │
                    ┌──────────▼──────────┐
                    │      Apache Kafka   │
                    │      (KRaft mode)   │
                    │         :9092       │
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │      Kafka UI       │
                    │         :8080       │
                    └─────────────────────┘
```

### Services

| Service | Port | Role |
|---------|------|------|
| `kafka` | 9092 | Broker in KRaft mode (no Zookeeper) |
| `kafka-ui` | 8080 | Web UI for browsing topics and messages |
| `order-service` | 8081 | Producer: accepts HTTP request, publishes `OrderCreatedEvent` |
| `notification-service` | 8082 | Consumer: reads `OrderCreatedEvent`, simulates sending an email |

---

## How to run

```bash
docker compose -f docker-compose-01.yml up --build
```

The first run downloads images and compiles services (5–7 minutes).
Subsequent runs are fast.

Check readiness:
Look at docker-compose or run the command
```bash
docker compose -f docker-compose-01.yml ps
# all 4 services should be in "healthy" or "running" state
```

---

## How to test

### 1. Create an order

```bash
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "user-42",
    "product": "Kafka: The Definitive Guide",
    "quantity": 2,
    "totalAmount": 59.99
  }' | jq
```
or on Windows
```cmd
curl -s -X POST http://localhost:8081/api/orders ^
  -H "Content-Type: application/json" ^
  -d "{\"userId\": \"user-42\", \"product\": \"Kafka: The Definitive Guide\", \"quantity\": 2, \"totalAmount\": 59.99}"
```

Response:
```json
{
  "orderId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "userId": "user-42",
  "product": "Kafka: The Definitive Guide",
  "quantity": 2,
  "totalAmount": 59.99,
  "timestamp": "2026-06-10T12:00:00"
}
```

### 2. Check notification-service logs

```bash
docker logs b01-notification-service --tail=20
```

Expected output:
```
──────────────────────────────────────
 NOTIFICATION #1
 Partition: 0  |  Offset: 0
 Order ID  : f47ac10b-58cc-4372-a567-0e02b2c3d479
 User ID   : user-42
 Product   : Kafka: The Definitive Guide x2
 Total     : $59.99
 Sent at   : 2026-06-10T12:00:00
 → Email sent to user user-42
──────────────────────────────────────
```

### 3. Check the received message counter

```bash
curl http://localhost:8082/api/notifications/count
# {"received": 1}
```

### 4. Browse messages in Kafka UI

Open in browser: [http://localhost:8080](http://localhost:8080)

- Topics → `01.orders.created` → Messages — shows JSON of each message
- Consumers → `notification-service-group` — shows offset and lag

---

## How it works internally

### Producer (order-service)

1. `OrderController` receives the POST request and passes data to `OrderService`.
2. `OrderService` creates an `OrderCreatedEvent` with a unique `orderId` (UUID) and current timestamp.
3. `KafkaTemplate.send(topic, key, value)` serializes the event to JSON and sends it to the `01.orders.created` topic.
   - **Key**: `orderId` — has no effect on routing yet (1 partition), but prepares us for branch03.
   - **Value**: JSON without type headers (`spring.json.add.type.headers=false`).
4. A `CompletableFuture` callback logs the partition and offset after broker acknowledgement.

### Kafka (broker)

- Runs in **KRaft mode**: uses its own Raft consensus instead of Zookeeper (`PROCESS_ROLES=broker,controller`).
- The `01.orders.created` topic is created automatically via a Spring `NewTopic` bean on order-service startup.
- 1 partition, replication factor 1 (single-broker dev setup).

### Consumer (notification-service)

1. `@KafkaListener(topics = ["01.orders.created"], groupId = "notification-service-group")` subscribes to the topic on startup.
2. `auto-offset-reset: earliest` — if the consumer group is new or offset is not found, reads from the beginning.
3. The method receives `ConsumerRecord<String, OrderCreatedEvent>` — includes metadata (partition, offset) and the event itself.
4. `JsonDeserializer` with `spring.json.value.default.type` deserializes the JSON into the local `OrderCreatedEvent` class.

### Why two separate OrderCreatedEvent classes?

Both `order-service` and `notification-service` have a class with the same name but in different packages.
This is intentional: in a real system, services are independent applications that should not share code via a shared library (for now).
Starting from branch11 (Avro + Schema Registry) we solve this problem through schema contracts.

---

## Why KRaft instead of Zookeeper?

Apache Kafka 3.3+ supports **KRaft** (Kafka Raft) — a built-in consensus mechanism.
From Kafka 4.0, Zookeeper has been completely removed. KRaft offers:
- Simpler deployment (one process instead of two)
- Faster startup and shutdown
- Better scalability for large clusters

---

## Key concepts in this branch

| Concept | What the example demonstrates |
|---------|-------------------------------|
| **Topic** | `01.orders.created` — a channel for one type of event |
| **Producer** | `KafkaTemplate.send()` — publishing a message |
| **Consumer** | `@KafkaListener` — subscribing to a topic |
| **Consumer Group** | `notification-service-group` — a logical group of readers |
| **Message Key** | `orderId` as the message key |
| **Offset** | Sequential number of a message within a partition |
| **KRaft mode** | Kafka without Zookeeper |
| **JsonSerializer** | Serializing a Kotlin data class to JSON |

---

## Project structure

```
kafka-laboratory/
├── docker-compose-01.yml
├── README01.md
└── branch01_basic/
    ├── order-service/
    │   ├── Dockerfile
    │   ├── build.gradle.kts
    │   ├── settings.gradle.kts
    │   └── src/main/kotlin/com/kafkalab/order/
    │       ├── OrderServiceApplication.kt
    │       ├── config/KafkaTopicConfig.kt      ← topic creation
    │       ├── controller/OrderController.kt   ← POST /api/orders
    │       ├── model/
    │       │   ├── CreateOrderRequest.kt
    │       │   └── OrderCreatedEvent.kt
    │       └── service/OrderService.kt         ← KafkaTemplate.send()
    └── notification-service/
        ├── Dockerfile
        ├── build.gradle.kts
        ├── settings.gradle.kts
        └── src/main/kotlin/com/kafkalab/notification/
            ├── NotificationServiceApplication.kt
            ├── controller/NotificationController.kt  ← GET /api/notifications/count
            ├── listener/OrderEventListener.kt        ← @KafkaListener
            └── model/OrderCreatedEvent.kt
```

---

## What's next — branch02

In the next branch we explore topics in more detail:
- Multiple topics for different event types
- Naming conventions
- Partition and retention configuration via CLI
- `kafka-topics.sh` commands