# Branch 13 — Saga Pattern (Choreography)

## 1. What we learn in this branch

- **Choreography Saga** — distributed transaction pattern without a central coordinator: each service
  reacts to other services' events and independently decides what to publish next.
- **Compensating Transactions** — when any step fails, previous steps are rolled back through
  dedicated compensation events (refund, cancel).
- **Idempotency Key** — the `idempotencyKey` field in every event protects against duplicate processing on retry:
  `processedKeys.add(key)` returns `false` for duplicates.
- **Distributed transactions without 2PC** — no locking, no Single Point of Failure.
- **8 Kafka topics** for a single "order" business process.
- **Avro + Schema Registry** — strict typing of all events between 4 services.

---

## 2. Changes compared to the previous branch (branch12)

- **Added** inventory-service — a new fourth microservice.
- **Instead of 2 topics** (orders.created, payments.processed), now **8 topics** for the full saga flow.
- **New Avro schemas**: PaymentFailedEvent, PaymentRefundedEvent, InventoryReservedEvent,
  InventoryFailedEvent, OrderConfirmedEvent, OrderCancelledEvent.
- **Removed** Kafka Transactions (`executeInTransaction`) — sagas use a different approach to consistency.
- **Added** idempotency key check in PaymentService.
- **Architecture** changed from a linear chain to a compensation graph.

---

## 3. Architecture

```
POST /api/orders
      │
      ▼
order-service
  ──[13.orders.created]──────────────────────► payment-service
                                                     │
                                        OK → [13.payments.processed]
                                       FAIL → [13.payments.failed] ──────────► order-service
                                                     │                           (cancel)
                                                     ▼
                                              inventory-service
                                                     │
                                        OK → [13.inventory.reserved] ──► order-service
                                                                           (confirm)
                                       FAIL → [13.inventory.failed] ──── payment-service
                                                                       └── order-service
                                                     │
                                              payment-service
                                              [13.payments.refunded]

notification-service listens to ALL 8 topics and logs the current saga step
```

**Topics and their participants:**
- `13.orders.created` (3 partitions) — producer: order; consumers: payment, notification
- `13.payments.processed` — producer: payment; consumers: inventory, notification
- `13.payments.failed` — producer: payment; consumers: order, notification
- `13.payments.refunded` — producer: payment; consumer: notification
- `13.inventory.reserved` — producer: inventory; consumers: order, notification
- `13.inventory.failed` — producer: inventory; consumers: payment, order, notification
- `13.orders.confirmed` — producer: order; consumer: notification
- `13.orders.cancelled` — producer: order; consumer: notification

**Services and ports (from docker-compose-13.yml):**
- `kafka` — 9092 (internal, KRaft single-node)
- `schema-registry-b13` — 8090
- `kafka-ui` — 8080 → http://localhost:8080
- `order-service-b13` — 8081 → http://localhost:8081
- `payment-service-b13` — 8083 → http://localhost:8083
- `inventory-service-b13` — 8085 → http://localhost:8085
- `notification-service-b13` — 8082 → http://localhost:8082

---

## 4. Key Concepts

### Choreography vs Orchestration

```
Choreography (branch13):
  Services react to events on their own — "I saw event X, so I publish Y"
  Coupling: loose (each service knows only its own input/output topics)
  Debugging: harder — flow is distributed across services

Orchestration (alternative):
  Central orchestrator gives commands — "do step 1, now step 2"
  Coupling: strong (orchestrator knows all participants)
  Debugging: easier — all state in one place
  But: orchestrator is a potential SPOF
```

### Compensating Transactions

```
Happy path: order.created → payment.processed → inventory.reserved → order.confirmed

Compensation (inventory fail):
  inventory.failed → payment.refunded (compensates payment)
                   → order.cancelled  (compensates order)

Compensation (payment fail):
  payment.failed → order.cancelled (compensates order)

Important: compensating transactions MUST be idempotent
```

### Idempotency Key

```kotlin
// payment-service: protection against duplicate processing on retry
private val processedKeys = ConcurrentHashMap.newKeySet<String>()

fun handleOrderCreated(event: OrderCreatedEvent) {
    if (!processedKeys.add(event.idempotencyKey.toString())) {
        log.warn("Duplicate idempotencyKey — skipped: {}", event.idempotencyKey)
        return
    }
    // ... process payment ...
}
```

`idempotencyKey = "order-created:{orderId}"` — unique per order. On retry the same
key is already in `processedKeys` → processing is skipped.

### Saga State Machine

```
OrderService internal state:
  PENDING → (payment ok) → PAYMENT_APPROVED
          → (inventory ok) → CONFIRMED
          → (payment fail) → CANCELLED
          → (inventory fail) → CANCELLED

API: GET /api/orders/{orderId}/status returns the current state
```

---

## 5. How to Run

```bash
# Start all services
docker compose -f docker-compose-13.yml up --build

# First run takes ~3 min (building 4 services + Avro generation)
# Kafka UI: http://localhost:8080
```

**Verify everything is ready:**
```bash
# All 4 services must be UP
docker compose -f docker-compose-13.yml ps

# Verify that 8 topics were created (auto.create.topics=false!)
curl http://localhost:8080/api/clusters/branch13/topics | jq '.[].name'
```

---

## 6. How to Test

### Scenario 1 — Happy Path

```bash
# Send an order
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-42", "itemCount": 2}' | jq .

# notification-service log (in order):
# [SAGA 1/5] ORDER CREATED     orderId=...
# [SAGA 2/5] PAYMENT APPROVED  orderId=...
# [SAGA 3/5] INVENTORY RESERVED orderId=...
# [SAGA 4/5] ORDER CONFIRMED   orderId=...
# [SAGA DONE ✅]

# Check order status
curl -s http://localhost:8081/api/orders/{orderId}/status | jq .
# {"status": "CONFIRMED"}
```

### Scenario 2 — Compensation: payment failure

```bash
# The next payment will fail
curl -s -X POST "http://localhost:8083/api/payments/fail-next?count=1"

# Send an order
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-99"}' | jq .

# notification-service log:
# [SAGA 1/5] ORDER CREATED
# [SAGA COMP] PAYMENT FAILED → compensation triggered
# [SAGA DONE ❌] ORDER CANCELLED reason=Payment failed
```

### Scenario 3 — Compensation: item out of stock

```bash
# The next reservation will fail
curl -s -X POST "http://localhost:8085/api/inventory/out-of-stock?count=1"

# Send an order
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-77"}' | jq .

# notification-service log:
# [SAGA 2/5] PAYMENT APPROVED
# [SAGA COMP] INVENTORY FAILED → payment refunded + order cancelled
# [SAGA DONE ❌] ORDER CANCELLED reason=Out of stock
```

### Scenario 4 — Verify idempotency key

```bash
# Send the same order twice (simulating retry)
BODY='{"userId": "user-1"}'
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" -d "$BODY" | jq .orderId

# Resend the same message via kafka-producer (or call the API twice)
# payment-service log: "Duplicate idempotencyKey — skipped"
```

---

## 7. Deep Dive

### Why 8 topics?

Each topic is a contract between two services. 8 topics = 8 business events in the order process.
Unlike REST (synchronous call), Kafka topics provide:
- **Decoupling** — services do not know each other's addresses
- **Durability** — messages are stored even if the service is down
- **Replay** — can re-read from any offset

### Choreography problems at scale

```
With 5+ services the dependency graph becomes complex:
  - Hard to understand the full flow without a diagram
  - Testing requires starting all services
  - Deadlines/timeouts between steps are hard to guarantee

Solution: add a Saga State Store (Redis/DB) in each service
  or switch to Orchestration (Temporal, Conductor)
```

### Avro schemas for compensation events

```json
// InventoryFailedEvent.avsc
{
  "type": "record",
  "name": "InventoryFailedEvent",
  "fields": [
    {"name": "orderId", "type": "string"},
    {"name": "paymentId", "type": "string"},
    {"name": "reason", "type": "string"},
    {"name": "occurredAt", "type": "long"}
  ]
}
```

`paymentId` is passed in `InventoryFailedEvent` so that payment-service knows which payment to refund.
This is an example of **event enrichment**: the event carries enough data for compensation without querying the DB.

### Schema Registry for 4 services

```yaml
# each service in application.yml
spring:
  kafka:
    properties:
      schema.registry.url: ${SCHEMA_REGISTRY_URL:http://localhost:8090}
    producer:
      value-serializer: io.confluent.kafka.serializers.KafkaAvroSerializer
    consumer:
      value-deserializer: io.confluent.kafka.serializers.KafkaAvroDeserializer
      properties:
        specific.avro.reader: true
```

---

## 8. Project Structure

```
branch13_saga_pattern/
├── order-service/
│   ├── src/main/kotlin/.../
│   │   ├── controller/OrderController.kt     ← POST /api/orders, GET /status
│   │   ├── service/OrderService.kt           ← Saga init, state machine
│   │   ├── listener/
│   │   │   ├── InventoryReservedListener.kt  ← publish orders.confirmed
│   │   │   ├── InventoryFailedListener.kt    ← publish orders.cancelled
│   │   │   └── PaymentFailedListener.kt      ← publish orders.cancelled
│   │   └── config/KafkaTopicConfig.kt        ← 8 TopicBuilder
│   └── src/main/avro/
│       ├── OrderCreatedEvent.avsc
│       ├── OrderConfirmedEvent.avsc
│       └── OrderCancelledEvent.avsc
├── payment-service/
│   ├── listener/OrderCreatedListener.kt      ← idempotency check, approve/fail
│   ├── listener/InventoryFailedListener.kt   ← refund payment
│   └── src/main/avro/
│       ├── PaymentProcessedEvent.avsc
│       ├── PaymentFailedEvent.avsc
│       └── PaymentRefundedEvent.avsc
├── inventory-service/
│   ├── listener/PaymentProcessedListener.kt  ← reserve/fail
│   └── src/main/avro/
│       ├── InventoryReservedEvent.avsc
│       └── InventoryFailedEvent.avsc
└── notification-service/
    └── listener/                             ← listens to all 8 topics, logs saga step
```

---

## 9. What's Next

- **Branch 14** — Kafka Streams: instead of listening to each topic separately — process the stream via
  KStream/KTable, aggregations, tumbling windows, state stores.
- **Branch 19** — Production-like demo: branch13 + SASL/ACL + cluster + monitoring.

---

## 10. Lecture Slides

### Slide 1 — The Problem of Distributed Transactions

```
What if a transaction is needed between two microservices?

Option 1: 2PC (Two-Phase Commit)
  ✗ Blocking protocol
  ✗ Coordinator — SPOF
  ✗ Scales poorly

Option 2: Saga Pattern
  ✓ Non-blocking
  ✓ No SPOF
  ✓ Each service executes a local transaction
  ! On failure — compensating transactions are required
```

### Slide 2 — Choreography Saga Flow

```
Choreography = "no conductor"
Each service knows: "if I see event X — I publish Y"

order ──[created]──► payment ──[processed]──► inventory ──[reserved]──► order ──[confirmed]

On inventory failure:
  inventory ──[failed]──► payment ──[refunded]
                      └──► order ──[cancelled]
```

### Slide 3 — Idempotency Key

```
Problem: Kafka may deliver a message twice (at-least-once)
If payment-service processes it twice → double charge!

Solution: idempotencyKey = "order-created:{orderId}"
  First time: processedKeys.add(key) → true  → process ✓
  Retry:      processedKeys.add(key) → false → skip ✓

In production: store processedKeys in Redis/DB (not in-memory!)
```

---

## 11. Demo Script

```bash
#!/bin/bash
# Branch 13 — Saga Pattern Demo

echo "=== Start ==="
docker compose -f docker-compose-13.yml up --build -d
echo "Waiting 3 minutes..."
sleep 180

echo ""
echo "=== Scenario 1: Happy Path ==="
RESP=$(curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-1","itemCount":2}')
echo "Response: $RESP"
ORDER_ID=$(echo $RESP | jq -r '.orderId')
sleep 2
echo "Status: $(curl -s http://localhost:8081/api/orders/$ORDER_ID/status)"

echo ""
echo "=== Scenario 2: Payment Failure ==="
curl -s -X POST "http://localhost:8083/api/payments/fail-next?count=1"
RESP=$(curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" -d '{"userId":"user-2"}')
echo "Response: $RESP"
ORDER_ID=$(echo $RESP | jq -r '.orderId')
sleep 2
echo "Status: $(curl -s http://localhost:8081/api/orders/$ORDER_ID/status)"

echo ""
echo "=== Scenario 3: Inventory Failure ==="
curl -s -X POST "http://localhost:8085/api/inventory/out-of-stock?count=1"
RESP=$(curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" -d '{"userId":"user-3"}')
echo "Response: $RESP"
ORDER_ID=$(echo $RESP | jq -r '.orderId')
sleep 3
echo "Status: $(curl -s http://localhost:8081/api/orders/$ORDER_ID/status)"

echo ""
echo "=== Kafka UI: http://localhost:8080 ==="
echo "=== View 8 topics and message flow ==="
```

---

## 12. Self-Check Questions

- How does Choreography Saga differ from Orchestration Saga?
- Why is an idempotency key needed? What happens without it on retry?
- If inventory.failed is received by both payment-service and order-service simultaneously — is there a race condition?
- How many Kafka messages does one happy path generate? One compensation path?
- Why is `AUTO_CREATE_TOPICS_ENABLE=false` a best practice?
- What is the difference between a `compensating transaction` and a regular rollback?
- How does the Avro schema protect against incompatible changes in InventoryFailedEvent?
- What does Schema Registry store and what is its role during deserialization?