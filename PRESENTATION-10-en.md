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

# Branch 10
# JSON Serialization

**Apache Kafka for Certification & Production**

---

## Agenda

1. The FQCN problem — `ClassNotFoundException` across services
2. `spring.json.type.mapping` — alias instead of FQCN
3. `spring.json.trusted.packages` — security whitelist
4. `spring.json.add.type.headers` — the mechanism
5. Event versioning — `eventVersion` field
6. Nested objects — `List<OrderItem>`
7. Multi-topic pipeline: order → payment → notification
8. DLT → business event pattern
9. Key Takeaways & What's Next

---

## The FQCN Problem — ClassNotFoundException

**Without type mapping: producer writes full class name into `__TypeId__` header**

```
Producer (order-service) sends:
  __TypeId__ = "com.kafkalab.order.model.OrderCreatedEvent"

Consumer (payment-service) tries to deserialize:
  → looks for "com.kafkalab.order.model.OrderCreatedEvent"
  → class NOT in payment-service classpath
  → ClassNotFoundException → consumer crashes
```

<br>

**Root cause:** tight coupling through fully-qualified class names (FQCN)
**Consequence:** rename any package → all consumers break

> Services must not share classpath. They must share a **contract** instead.

---

## spring.json.type.mapping — Alias Solution

**Replace FQCN with a short alias — each service maps it locally:**

```yaml
# order-service (producer):
spring.kafka.producer.properties:
  spring.json.type.mapping: >
    OrderCreatedEvent:com.kafkalab.order.model.OrderCreatedEvent

# payment-service (consumer):
spring.kafka.consumer.properties:
  spring.json.type.mapping: >
    OrderCreatedEvent:com.kafkalab.payment.model.OrderCreatedEvent
```

```
Producer sends: __TypeId__ = "OrderCreatedEvent"  ← short alias
Consumer reads: "OrderCreatedEvent" → maps to its own local class ✓
```

<br>

> Services share the **alias**, not the class path.
> Each service has its own local copy of the event class.

---

## spring.json.trusted.packages

**Security: whitelist which packages are allowed for deserialization**

```yaml
# Development (permissive):
spring.json.trusted.packages: "*"

# Production (restrictive):
spring.json.trusted.packages: "com.kafkalab.payment.model,com.kafkalab.shared"
```

<br>

**Why it matters:**
- Without restriction: deserialization of arbitrary classes = security vulnerability
- Java deserialization attacks exploit unrestricted type resolution
- With `"*"`: any class name in `__TypeId__` header could be instantiated

```
Without trusted.packages → ClassNotTrustedException
(even for legitimate classes not in the whitelist)
```

> Always specify concrete packages in production. Never use `"*"` in production.

---

## Event Versioning

**Add `eventVersion` to every event — enables backward-compatible evolution:**

```kotlin
data class OrderCreatedEvent(
    val orderId: String,
    val userId: String,
    val totalAmount: Double,
    val items: List<OrderItem> = emptyList(),
    val eventVersion: String = "1.0"    // ← version field with default
)
```

<br>

```
Consumer logic:
  when (event.eventVersion) {
      "1.0" → handle v1 format
      "2.0" → handle v2 format with new fields
      else  → log warning, use defaults
  }
```

> Version field + default value = backward-compatible rolling deployment.
> Old consumers handle v1 messages; new consumers handle both.

---

## Nested Objects — List<OrderItem>

**Jackson serializes nested objects automatically — no extra configuration:**

```kotlin
data class OrderItem(
    val productId: String,
    val name: String,
    val quantity: Int,
    val price: Double
)

data class OrderCreatedEvent(
    val orderId: String,
    val items: List<OrderItem> = emptyList()  // ← default = backward compatible
)
```

```json
{
  "orderId": "order-001",
  "items": [
    {"productId": "p1", "name": "Book", "quantity": 2, "price": 19.99},
    {"productId": "p2", "name": "Pen",  "quantity": 1, "price":  2.99}
  ]
}
```

---

## Multi-Topic Pipeline

**Three services, three topics, one business flow:**

```
POST /api/orders
      │
      ▼
order-service ──[10.orders.created]──► payment-service
                                             │
                          OK → [10.payments.processed(APPROVED)] ──► notification
                        FAIL → [10.payments.processed(DECLINED)] ──► notification
                                             │
                                     (on DLT exception)
                                       @DltHandler
                                    publish DECLINED event ──────────► notification
```

- `notification-service` listens to **3 topics**: `orders.created`, `orders.cancelled`, `payments.processed`
- Single `type.mapping` in notification for 3 different event types
- Each service independently deployable

---

## DLT → Business Event Pattern

**Evolving from logging to business-meaningful DLT handling:**

```kotlin
// branch09: DLT handler just logs
@DltHandler
fun handleDlt(event: OrderCreatedEvent) {
    log.error("DLT: {}", event.orderId)
}

// branch10: DLT handler publishes a business event
@DltHandler
fun handleDlt(event: OrderCreatedEvent) {
    log.error("DLT: payment failed for order {}", event.orderId)
    val declinedEvent = PaymentProcessedEvent(
        orderId = event.orderId,
        status = "DECLINED",
        reason = "Payment processing exhausted all retries"
    )
    kafkaTemplate.send("10.payments.processed", event.userId, declinedEvent)
}
```

> notification-service receives DECLINED and notifies the user.
> Business flow completes correctly even on technical failure.

---

## Key Takeaways

- **`spring.json.type.mapping`** = mandatory for cross-service Kafka in Java/Kotlin
- **`trusted.packages`** = security best practice for production
- **Alias decoupling** — services share the alias string, not the class path
- **Event versioning** — add from day one; retrofitting is expensive
- **Default values** in event classes = backward-compatible schema evolution
- **DLT as business event** — better than silent logging, completes the business flow
- Each service has its **own local copy** of event classes (loose coupling)

<br>

> Serialization is the contract between producer and consumer. Treat it seriously.

---

## What's Next — Branch 11

**Schema Registry & Avro — structured contracts**

- Apache Avro — binary format, strict typing, ~3.5x smaller than JSON
- Schema Registry — central schema storage with compatibility checks
- Schema evolution: BACKWARD, FORWARD, FULL
- `schema-id` in payload — no need for `__TypeId__` header
- `specific.avro.reader` — generated classes vs `GenericRecord`

---

<!-- _class: title -->

## What's Next — Branch 11
- Avro: binary format, 80 bytes vs 300 bytes JSON
- Schema Registry: compile-time + runtime contracts
- Schema evolution with compatibility modes
- SchemaRegistryException blocks incompatible changes

# Questions?

**Branch 10 — JSON Serialization**