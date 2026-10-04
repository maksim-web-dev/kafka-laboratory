# Branch 10 — JSON Serialization

## `branch10_json_serialization` — what we learn

- **`JsonSerializer` / `JsonDeserializer`** — Spring Kafka built-in serialization via Jackson.
- **`__TypeId__` header** — Kafka header containing class type information for deserialization.
- **`spring.json.add.type.headers: true`** — producer adds `__TypeId__` to every message.
- **`spring.json.type.mapping`** — alias instead of FQCN for safe cross-service mapping.
- **`spring.json.trusted.packages`** — whitelist of packages for safe deserialization.
- **Event versioning** — `eventVersion` field in the model for backward compatibility.
- **Nested objects** — `List<OrderItem>` inside `OrderCreatedEvent` (serialized automatically).
- **`PaymentProcessedEvent`** — new event type with `APPROVED` / `DECLINED` status.
- **Multi-topic pipeline** — three-service chain: `orders.created` → payment → `payments.processed` → notification.
- **DLT → business event** — `@DltHandler` publishes `PaymentProcessedEvent(DECLINED)` instead of silent logging.

## What changed compared to branch09

- Restored `notification-service` — now a three-service pipeline.
- `OrderCreatedEvent` model extended: `+eventVersion`, `+items: List<OrderItem>`.
- New type `OrderItem` added (nested object).
- New topic `10.payments.processed` — for communication between payment and notification.
- New event type `PaymentProcessedEvent` with `APPROVED`/`DECLINED` status.
- `@DltHandler` now publishes `PaymentProcessedEvent(DECLINED)` instead of silent logging.
- `notification-service` listens to 3 topics: `orders.created`, `orders.cancelled`, `payments.processed`.
- `spring.json.type.mapping` configured in all services for safe mapping.
- Topic names changed to `10.orders.*`, `10.payments.*`.

## Architecture

```
order-service-b10 :8081
  createOrder() → OrderCreatedEvent{eventVersion, items: [OrderItem]}
          │
     10.orders.created (3 partitions)
     10.orders.cancelled (1 partition)
          │                   │
          ▼                   ▼
payment-service-b10 :8083    notification-service-b10 :8082
  @RetryableTopic             handleOrderCancelled()
  handleOrderCreated()
    APPROVED → PaymentProcessedEvent
    DECLINED → @DltHandler → PaymentProcessedEvent(DECLINED)
          │
     10.payments.processed (3 partitions)
          │
          ▼
notification-service-b10 :8082
  handlePaymentProcessed()
  logs APPROVED / DECLINED

__TypeId__ header in every message:
  10.orders.created   → __TypeId__: OrderCreatedEvent
  10.payments.processed → __TypeId__: PaymentProcessedEvent
```

### Topics and their configuration

- `10.orders.created` → partitions: 3, retention: 7 days, producer: order-service.
- `10.orders.cancelled` → partitions: 1, retention: 7 days, producer: order-service.
- `10.payments.processed` → partitions: 3, retention: 7 days, producer: payment-service.
- `10.orders.created-retry-0`, `10.orders.created-retry-1`, `10.orders.created-dlt` → auto-created.

## Key Concepts of This Branch

### The Problem with FQCN in `__TypeId__`

Without `spring.json.type.mapping` the producer writes the fully qualified class name:

```
__TypeId__: com.kafkalab.order.model.OrderCreatedEvent
```

A consumer in another service (e.g. `payment-service`) receives this header
and tries to find the class `com.kafkalab.order.model.OrderCreatedEvent` in its own classpath.
Class not found → `ClassNotFoundException` → deserialization error.

### Solution: spring.json.type.mapping

Producer writes an alias instead of the FQCN:

```yaml
# order-service/application.yml (producer)
spring.json.type.mapping: "OrderCreatedEvent:com.kafkalab.order.model.OrderCreatedEvent"
# Header: __TypeId__: OrderCreatedEvent  ← alias, not FQCN
```

Each consumer service has its own mapping from alias to local class:

```yaml
# payment-service/application.yml (consumer)
spring.json.type.mapping: "OrderCreatedEvent:com.kafkalab.payment.model.OrderCreatedEvent"
# "OrderCreatedEvent" → local class of payment-service

# notification-service/application.yml (consumer)
spring.json.type.mapping: "OrderCreatedEvent:com.kafkalab.notification.model.OrderCreatedEvent,
  OrderCancelledEvent:com.kafkalab.notification.model.OrderCancelledEvent,
  PaymentProcessedEvent:com.kafkalab.notification.model.PaymentProcessedEvent"
```

> Each service has its own copy of the class with the same name but in its own package.
> They do not depend on each other (loose coupling).

### spring.json.trusted.packages

Without `trusted.packages` the deserializer will reject any class for security reasons:

```yaml
spring:
  kafka:
    consumer:
      properties:
        spring.json.trusted.packages: "*"  # allow all (for dev/test)
        # or:
        spring.json.trusted.packages: "com.kafkalab.payment.model"  # only this package
```

In production it is better to specify concrete packages rather than `"*"`.

### Event versioning

```kotlin
// OrderCreatedEvent.kt — branch10
data class OrderCreatedEvent(
    val eventVersion: String = "1.0",     // ← NEW: schema version
    val orderId: String = UUID.randomUUID().toString(),
    val userId: String = "",
    val items: List<OrderItem> = emptyList(),  // ← NEW: nested list
    val totalAmount: Double = 0.0,
    val timestamp: String = LocalDateTime.now().toString()
)

// OrderItem.kt — NEW
data class OrderItem(
    val productId: String = UUID.randomUUID().toString(),
    val productName: String = "",
    val quantity: Int = 1,
    val unitPrice: Double = 0.0
)
```

The `eventVersion` field allows the consumer to identify the schema version and handle accordingly:

```kotlin
if (event.eventVersion == "2.0") {
    // new logic
} else {
    // backward compatible logic
}
```

### Nested objects

Jackson serializes `List<OrderItem>` as a JSON array — no separate `type.mapping` needed:

```json
{
  "eventVersion": "1.0",
  "orderId": "uuid",
  "userId": "alice",
  "items": [
    { "productId": "uuid", "productName": "Laptop", "quantity": 1, "unitPrice": 1500.0 }
  ],
  "totalAmount": 1500.0
}
```

### DLT → Business Event (DECLINED)

In branch09 `@DltHandler` only logged.
Now it publishes `PaymentProcessedEvent(status=DECLINED)` to `10.payments.processed`:

```kotlin
// OrderPaymentListener.kt
@DltHandler
fun handleDlt(record: ConsumerRecord<String, OrderCreatedEvent>) {
    val event = record.value()
    val declined = PaymentProcessedEvent(
        orderId = event.orderId,
        userId = event.userId,
        amount = event.totalAmount,
        status = PaymentStatus.DECLINED,
        paymentId = "DECLINED-${UUID.randomUUID()}"
    )
    kafkaTemplate.send("10.payments.processed", event.userId, declined)
    log.error("[DLT] Order {} → published DECLINED event", event.orderId)
}
```

`notification-service` receives `DECLINED` and logs accordingly:
`"⚠ Payment DECLINED for order {orderId} — user {userId} notified"`.

## How to Run

```bash
docker compose -f docker-compose-10.yml up --build
```

Verify 5 containers:

```bash
docker compose -f docker-compose-10.yml ps
# kafka, kafka-ui, order-service-b10, payment-service-b10, notification-service-b10
```

## How to Test

### 1. Create an order — verify eventVersion and items

```bash
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"alice","product":"Laptop","quantity":1,"totalAmount":1500.0}'
```

Expected response (now includes `eventVersion` and `items`):

```json
{
  "eventVersion": "1.0",
  "orderId": "uuid",
  "userId": "alice",
  "items": [{ "productName": "Laptop", "quantity": 1, "unitPrice": 1500.0 }],
  "totalAmount": 1500.0
}
```

### 2. Check payment-service and notification-service

```bash
curl http://localhost:8083/api/payments/stats
# {"processed":1,"dlt":0,"failNextN":0}

curl http://localhost:8082/api/notifications/count
# {"orders_created":0,"orders_cancelled":0,"payments_processed":1,"total":1}
```

### 3. Simulate payment failure → DLT → DECLINED

```bash
# Schedule 3 failures → message goes retry-0 → retry-1 → DLT → DECLINED
curl -s -X POST http://localhost:8083/api/payments/simulate-failure/3

curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"bob","product":"Headphones","quantity":1,"totalAmount":200.0}'

# After ~7s check:
curl http://localhost:8083/api/payments/stats
# {"processed":0,"dlt":1}

curl http://localhost:8082/api/notifications/count
# payments_processed: 1 (DECLINED notification)
```

**Windows (PowerShell):**

```powershell
Invoke-RestMethod -Method POST -Uri http://localhost:8083/api/payments/simulate-failure/3
Invoke-RestMethod -Method POST -Uri http://localhost:8081/api/orders `
  -ContentType "application/json" `
  -Body '{"userId":"bob","product":"Headphones","quantity":1,"totalAmount":200.0}'
```

### 4. Check the `__TypeId__` header in Kafka UI

Open: `http://localhost:8080`

- `Topics` → `10.orders.created` → any message → **Headers** tab:
  - `__TypeId__: OrderCreatedEvent` ← alias
- `Topics` → `10.payments.processed` → Headers:
  - `__TypeId__: PaymentProcessedEvent`

## How It Works Internally

### Producer — order-service

```yaml
# order-service/application.yml — branch10
spring:
  kafka:
    producer:
      properties:
        spring.json.add.type.headers: true          # ← NEW: adds __TypeId__
        spring.json.type.mapping: "OrderCreatedEvent:com.kafkalab.order.model.OrderCreatedEvent,
          OrderCancelledEvent:com.kafkalab.order.model.OrderCancelledEvent"
```

```kotlin
// OrderCreatedEvent.kt — branch10 (new fields)
data class OrderCreatedEvent(
    val eventVersion: String = "1.0",
    val orderId: String = UUID.randomUUID().toString(),
    val userId: String = "",
    val items: List<OrderItem> = emptyList(),   // ← NEW
    val totalAmount: Double = 0.0,
    val timestamp: String = LocalDateTime.now().toString()
)
```

### Consumer — payment-service

```yaml
# payment-service/application.yml — consumer
spring.json.type.mapping: "OrderCreatedEvent:com.kafkalab.payment.model.OrderCreatedEvent"
spring.json.trusted.packages: "*"
```

### Consumer — notification-service (listens to 3 topics)

```kotlin
// NotificationListener.kt — branch10
@KafkaListener(topics = ["10.orders.created"], groupId = "notification-service-group")
fun handleOrderCreated(record: ConsumerRecord<String, OrderCreatedEvent>) {
    log.info("[NOTIFICATION] Order created: orderId={}", record.value().orderId)
}

@KafkaListener(topics = ["10.payments.processed"], groupId = "notification-service-group")
fun handlePaymentProcessed(record: ConsumerRecord<String, PaymentProcessedEvent>) {
    val event = record.value()
    if (event.status == "DECLINED") {
        log.warn("[NOTIFICATION] ⚠ Payment DECLINED for order {}", event.orderId)
    } else {
        log.info("[NOTIFICATION] ✓ Payment APPROVED for order {}", event.orderId)
    }
    paymentsProcessedCount.incrementAndGet()
}
```

## Project Structure (changes from branch09)

```
kafka-laboratory/
├── docker-compose-10.yml                          ← 5 containers, topics 10.*
├── branch10_json_serialization/
│   ├── order-service/
│   │   └── model/
│   │       ├── OrderCreatedEvent.kt               ← +eventVersion, +items: List<OrderItem>
│   │       └── OrderItem.kt                       ← NEW
│   ├── payment-service/
│   │   ├── model/
│   │   │   ├── PaymentProcessedEvent.kt           ← NEW
│   │   │   └── PaymentStatus.kt                   ← NEW enum APPROVED/DECLINED
│   │   └── listener/OrderPaymentListener.kt       ← @DltHandler now publishes DECLINED
│   └── notification-service/                      ← NEW service
│       └── listener/NotificationListener.kt       ← listens to 3 topics
│       └── src/main/resources/
│           └── application.yml                    ← 3 type.mapping entries
└── README10.md
```

## What's Next — branch11

- **Apache Avro** — binary serialization, `.avsc` schema files, Java class generation.
- **Confluent Schema Registry** — central schema store with versioning.
- **Backward / Forward / Full compatibility** — schema compatibility check modes.
- **Message size** — Avro ~80 bytes vs JSON ~300 bytes for the same data.
- **Schema evolution** — safely adding fields with `default` values.

---
---

## Presentation Slides (11 slides)

**Slide 1: Branch 10 — JSON Serialization**
- Apache Kafka for Certification & Production
- type.mapping, type headers, event versioning, pipeline

**Slide 2: Agenda**
1. The FQCN problem in __TypeId__ header
2. spring.json.type.mapping — alias instead of FQCN
3. spring.json.trusted.packages — whitelist
4. spring.json.add.type.headers
5. Event versioning — eventVersion
6. Nested objects — List<OrderItem>
7. Multi-topic pipeline
8. DLT → business event (DECLINED)
9. Key takeaways & CCDAK

**Slide 3: The Problem — ClassNotFoundException**
- JsonSerializer writes FQCN in __TypeId__: `com.kafkalab.order.model.OrderCreatedEvent`
- Consumer in another service does not know this class → ClassNotFoundException
- A solution is needed: loose coupling between producer and consumer

**Slide 4: spring.json.type.mapping**
- Producer: alias → FQCN (writes alias in __TypeId__)
- Consumer: alias → local FQCN (deserializes into its own class)
- Each service has its own copy of the class in its own package
- Changing namespace in the producer does not break consumers

**Slide 5: spring.json.trusted.packages**
- Protection against arbitrary deserialization (Java deserialization attack)
- `"*"` — allow all (dev/test)
- Production: specific packages `"com.kafkalab.payment.model"`
- Without trusted.packages — ClassNotTrustedException

**Slide 6: Event Versioning**
- `eventVersion: String = "1.0"` — schema version in every event
- Allows consumer to distinguish v1.0 and v2.0 messages
- Backward compatible: new field with a default value
- Required for rolling deployment without downtime

**Slide 7: Nested Objects**
- `List<OrderItem>` serialized by Jackson automatically
- No separate type.mapping needed for OrderItem
- Default value `emptyList()` — backward compatible
- JSON array in payload: `"items":[{...},{...}]`

**Slide 8: Multi-topic Pipeline**
- order → 10.orders.created → payment → 10.payments.processed → notification
- Each service is both producer and consumer simultaneously (except order and notification)
- notification listens to 3 topics: orders.created, orders.cancelled, payments.processed
- Single type.mapping in notification for 3 event types

**Slide 9: DLT → Business Event**
- Branch09: @DltHandler only logged
- Branch10: @DltHandler publishes PaymentProcessedEvent(DECLINED)
- notification-service receives DECLINED and notifies the user
- Business logic completes correctly even on failure — through DLT

**Slide 10: Key Takeaways**
- `spring.json.type.mapping` — mandatory for cross-service Kafka in Java/Kotlin
- `trusted.packages` — security best practice for production
- Event versioning — necessary for backward compatibility
- DLT → business event: a better DLT pattern than simple logging
- Each service has its own copy of event classes (loose coupling)

**Slide 11: What's Next — Branch 11: Schema Registry & Avro**
- Apache Avro — binary format, strict typing
- Schema Registry — central schema store
- Backward/Forward/Full compatibility
- Smaller message size: ~80 bytes vs ~300 bytes

## Presentation Script

**Slide 1:**
Branch ten — JSON Serialization.
Here we solve a real problem that arises in every microservice Kafka project:
how to safely pass typed objects between services where each has its own classpath?

**Slide 2:**
We will cover nine topics from ClassNotFoundException to a complete three-service pipeline.
Key insight: serialization is a contract between producer and consumer.

**Slide 3:**
Without type mapping Jackson writes the full class name in the __TypeId__ header.
When payment-service reads this message — it does not have com.kafkalab.order.model.OrderCreatedEvent.
ClassNotFoundException stops the consumer.
This is the typical error when first setting up cross-service Kafka.

**Slide 4:**
spring.json.type.mapping solves the problem through aliases.
Producer writes the short alias "OrderCreatedEvent" instead of the FQCN.
Each consumer service has its own mapping from alias to its own local class.
Services do not depend on each other's package — loose coupling.

**Slide 5:**
trusted.packages is a security mechanism.
Without it the deserializer may reject even legitimate classes.
"*" is convenient for development but unsafe in production.
In production — specify a concrete package or set of packages.

**Slide 6:**
eventVersion — a simple but effective versioning mechanism.
Producer sets "1.0" in every event.
When the schema changes — producer will start setting "2.0".
Consumer checks the version and handles accordingly — backward compatible rolling deployment.

**Slide 7:**
Nested objects in Kafka through JSON — simple and convenient.
List<OrderItem> is serialized by Jackson automatically without additional configuration.
Default emptyList() guarantees backward compatibility — old consumers do not break.
JSON array in the payload reads naturally.

**Slide 8:**
Three-service pipeline — a more realistic architecture.
order-service publishes orders, payment-service processes and publishes the result.
notification-service listens to both event types and notifies the user.
Each service is independent — can be deployed separately.

**Slide 9:**
The DLT pattern becomes more powerful when @DltHandler publishes a business event.
Instead of silent logging — payment-service publishes PaymentProcessedEvent(DECLINED).
notification-service receives it and notifies the user about the declined payment.
The business process completes correctly even on a technical error.

**Slide 10:**
Main takeaways: spring.json.type.mapping is mandatory for any multi-service Kafka setup.
trusted.packages — always specify concrete packages in production.
Event versioning — a good practice from day one, cheaper than migration later.
DLT as a business event rather than simple logging — the evolution of error handling.

**Slide 11:**
The next branch — Schema Registry and Avro.
Avro solves the same problems as JSON type.mapping but more efficiently.
Schema Registry — central schema store with compatibility checking.
Message size decreases from 300 to 80 bytes.

## Test Questions (up to 10 questions)

**Question 1:**
A consumer received a message with `__TypeId__: com.kafkalab.order.model.OrderCreatedEvent`.
The consumer service does not have this class. What will happen?

A) Spring Kafka automatically converts the type
B) ClassNotFoundException during deserialization → consumer error
C) The message will be skipped without error
D) Consumer will request the class from the producer over the network

**Answer:** B —
`__TypeId__` header tells JsonDeserializer which class to use.
If the class is not found in the classpath — `ClassNotFoundException`.

---

**Question 2:**
What is the purpose of `spring.json.type.mapping` in the producer?

A) Convert JSON to binary format
B) Write an alias instead of FQCN in the `__TypeId__` header
C) Disable the `__TypeId__` header
D) Add compression to the message

**Answer:** B —
`spring.json.type.mapping: "OrderCreatedEvent:com.kafkalab.order.model.OrderCreatedEvent"` →
producer writes `__TypeId__: OrderCreatedEvent` (alias) instead of FQCN.

---

**Question 3:**
`spring.json.trusted.packages: "*"` — what risk does this pose in production?

A) All messages will be rejected
B) An attacker can forge `__TypeId__` and cause a malicious class to be deserialized
C) Throughput will decrease due to additional checks
D) No risk — it is just configuration

**Answer:** B —
Without a whitelist an arbitrary `__TypeId__` in the header can trigger deserialization of a class
with undesired behavior (Java deserialization gadget chain).
In production: specify concrete packages.

---

**Question 4:**
`OrderCreatedEvent` in `order-service` has namespace `com.kafkalab.order.model`.
In `payment-service` the same class has namespace `com.kafkalab.payment.model`.
How do they communicate through Kafka?

A) A shared library with a single namespace is required
B) `spring.json.type.mapping` maps alias `OrderCreatedEvent` to the local class in each service
C) Spring Kafka automatically finds the correct class
D) Schema Registry is needed for the mapping

**Answer:** B —
Producer: alias → `com.kafkalab.order.model.OrderCreatedEvent`.
Consumer (payment): alias → `com.kafkalab.payment.model.OrderCreatedEvent`.
Each service has its own copy of the class — loose coupling.

---

**Question 5:**
`OrderCreatedEvent` has field `items: List<OrderItem>` with default `emptyList()`.
An old consumer (branch09) that does not know about `items` receives a new message.
What will happen?

A) ClassNotFoundException — `OrderItem` not found
B) Jackson ignores unknown fields by default → deserialization succeeds
C) Error: `items` is a required field
D) Consumer will get NullPointerException

**Answer:** B —
Jackson ignores unknown fields by default (`DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES=false`).
The old consumer receives the known fields; `items` is ignored.

---

**Question 6:**
What role does `eventVersion: String = "1.0"` play in the event model?

A) Kafka metadata — automatically handled by the broker
B) Allows consumers to distinguish schema versions and handle them backward-compatibly
C) A required field for Spring Kafka JsonDeserializer
D) Used for routing to different partitions

**Answer:** B —
`eventVersion` is a business field in the payload.
Allows consumer to check `if (event.eventVersion == "2.0")` and apply new logic.
During rolling deployment: old consumers handle "1.0", new consumers handle both "1.0" and "2.0".

---

**Question 7:**
`notification-service` listens to `10.payments.processed`.
`payment-service` publishes `PaymentProcessedEvent` with alias `PaymentProcessedEvent`.
What is needed in `notification-service/application.yml`?

A) Nothing — Spring Kafka deserializes automatically
B) `spring.json.type.mapping: "PaymentProcessedEvent:com.kafkalab.notification.model.PaymentProcessedEvent"`
C) `spring.json.type.mapping: "PaymentProcessedEvent:com.kafkalab.payment.model.PaymentProcessedEvent"`
D) `spring.json.trusted.packages: "com.kafkalab.payment.model"`

**Answer:** B —
notification-service has its own copy of `PaymentProcessedEvent` in its own package.
type.mapping maps alias → local class of the notification service.

---

**Question 8:**
Why does `@DltHandler` in branch10 publish `PaymentProcessedEvent(DECLINED)` instead of simple logging?

A) Spring Kafka requires publishing after DLT
B) So that notification-service can notify the user about the payment failure
C) To reduce lag in the DLT topic
D) For compatibility with Schema Registry

**Answer:** B —
Simple logging hides the failure inside the service.
Publishing DECLINED → notification-service receives the event → notifies the user.
The business process completes correctly even on a technical error.

---

**Question 9:**
`spring.json.add.type.headers: true` is set in one producer.
`spring.json.add.type.headers: false` is set in another producer for the same topic.
Consumer sees both types of messages. What will happen?

A) Consumer always deserializes successfully
B) Messages without `__TypeId__` → deserialization error if no type is specified explicitly
C) Spring Kafka automatically determines the type from the JSON structure
D) Messages without the header will be sent to DLT

**Answer:** B —
Without `__TypeId__` header the deserializer does not know which class to map JSON to.
Either set `ValueType` in the consumer configuration, or keep `add.type.headers: true`.

---

**Question 10:**
Both `APPROVED` and `DECLINED` `PaymentProcessedEvent` are published to `10.payments.processed`.
`notification-service` handles both. What configuration is needed?

A) Two separate `@KafkaListener` for APPROVED and DECLINED
B) One `@KafkaListener` for `10.payments.processed` + check `event.status` in code
C) Two separate topics: `10.payments.approved` and `10.payments.declined`
D) Filter at the `@KafkaListener` level with `condition="..."`

**Answer:** B —
One listener reads all messages from the topic.
Logic branching happens inside the listener: `if (event.status == "DECLINED") { ... }`.
This is the simplest and most efficient approach.