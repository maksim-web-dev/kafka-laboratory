# Branch 11 — Schema Registry & Avro

## `branch11_schema_registry` — what we learn

- **Apache Avro** — binary serialization with `.avsc` schema files and Java class generation.
- **Confluent Schema Registry** — HTTP service for storing, versioning, and validating schemas.
- **`KafkaAvroSerializer` / `KafkaAvroDeserializer`** — Confluent serializer for Avro.
- **`specific.avro.reader: true`** — deserializer uses the concrete generated class.
- **Gradle Avro Plugin** — `com.github.davidmc24.gradle.plugin.avro` generates Java classes from `.avsc`.
- **Schema evolution** — Backward / Forward / Full compatibility when the schema changes.
- **Message size** — Avro ~80 bytes vs JSON ~300 bytes for the same data.
- **Schema ID in payload** — every Avro message contains a 5-byte prefix: `0x00` + 4 bytes schema ID.
- **Schema Registry REST API** — `/subjects`, `/versions/latest`, `/compatibility`.
- **`OrderItem` as a nested Avro schema** — `"type": "array", "items": "com.kafkalab.avro.OrderItem"`.

## What changed compared to branch10

- `JsonSerializer` / `JsonDeserializer` → **`KafkaAvroSerializer` / `KafkaAvroDeserializer`**.
- Instead of Kotlin data classes → **generated Java classes** from Avro Gradle Plugin (`.avsc` → `.java`).
- `spring.json.type.mapping` → **`specific.avro.reader: true`** + schema ID in payload.
- `spring.json.add.type.headers` → **not needed** (schema ID is embedded in the payload).
- New container **`schema-registry-b11`** added on port **8090**.
- `schema.registry.url` configured in all three services.
- Schemas in `src/main/avro/` — `.avsc` files for each event type.
- Topic names changed to `11.orders.*`, `11.payments.*`.

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                     Schema Registry :8090                        │
│  subjects:                                                       │
│    11.orders.created-value   → schema OrderCreatedEvent v1       │
│    11.orders.cancelled-value → schema OrderCancelledEvent v1     │
│    11.payments.processed-value → schema PaymentProcessedEvent v1 │
└──────────────┬───────────────────────────────────────────────────┘
               │  registration on first publish
               │  schema ID retrieval on each publish
┌──────────────┴──────────────────────────────────────────────────┐
│  order-service :8081                                             │
│  KafkaAvroSerializer → registers schema → gets schema-id=1       │
│  payload: [0x00][0x00 0x00 0x00 0x01][avro binary data]         │
└────────────────────────┬────────────────────────────────────────┘
                         │
          ┌──────────────┴──────────────────┐
          │                                  │
   11.orders.created                  11.orders.cancelled
          │                                  │
┌─────────▼──────────────┐        ┌──────────▼───────────────────┐
│  payment-service :8083  │        │  notification-service :8082   │
│  KafkaAvroDeserializer  │        │  KafkaAvroDeserializer        │
│  specific.avro.reader   │        │  specific.avro.reader         │
└─────────┬───────────────┘        └──────────────────────────────┘
          │
   11.payments.processed
          │
┌─────────▼──────────────┐
│  notification-service   │
│  KafkaAvroDeserializer  │
└────────────────────────┘
```

### Topics and their configuration

- `11.orders.created` → partitions: 3, retention: 7 days, Avro schema: OrderCreatedEvent v1.
- `11.orders.cancelled` → partitions: 1, retention: 7 days, Avro schema: OrderCancelledEvent v1.
- `11.payments.processed` → partitions: 3, retention: 7 days, Avro schema: PaymentProcessedEvent v1.

## Key Concepts of This Branch

### Avro vs JSON — comparison

```
JSON message (OrderCreatedEvent):
{
  "eventVersion": "1.0",
  "orderId": "550e8400-e29b-41d4-a716-446655440000",
  "userId": "alice",
  "items": [{"productName":"Laptop","quantity":1,"unitPrice":1500.0}],
  "totalAmount": 1500.0,
  "timestamp": "2024-01-15T10:30:00"
}
→ ~280 bytes (field names in every message)

Avro message (the same OrderCreatedEvent):
[0x00][schema-id: 4 bytes][avro binary]
→ ~80 bytes (field names only in the schema; payload is pure bytes)
```

- Avro payload: 5-byte prefix + binary data without field names.
- Schema Registry: `schema-id=1` → fetch schema → deserialize.
- ~3.5x smaller size → less network traffic, less storage.

### .avsc schema and code generation

```json
// src/main/avro/OrderCreatedEvent.avsc
{
  "type": "record",
  "name": "OrderCreatedEvent",
  "namespace": "com.kafkalab.avro",
  "fields": [
    {"name": "eventVersion", "type": "string", "default": "1.0"},
    {"name": "orderId",      "type": "string", "default": ""},
    {"name": "userId",       "type": "string", "default": ""},
    {"name": "items",        "type": {"type": "array", "items": "com.kafkalab.avro.OrderItem"}, "default": []},
    {"name": "totalAmount",  "type": "double",  "default": 0.0},
    {"name": "timestamp",    "type": "string",  "default": ""}
  ]
}
```

Gradle Avro Plugin (`./gradlew generateAvroJava`) generates:

```java
// build/generated-main-avro-java/com/kafkalab/avro/OrderCreatedEvent.java
// (generated automatically — do not edit manually)
public class OrderCreatedEvent extends SpecificRecordBase implements SpecificRecord {
    private String eventVersion;
    private String orderId;
    // ...
    public static OrderCreatedEvent.Builder newBuilder() { ... }
}
```

### Schema Registry — registration and schema retrieval

```
Producer (first publish):
  1. KafkaAvroSerializer → HTTP POST /subjects/11.orders.created-value/versions
  2. Schema Registry → stores schema → returns schema-id=1
  3. Message: [0x00][0x00 0x00 0x00 0x01][binary data]

Producer (subsequent publishes):
  1. KafkaAvroSerializer → looks up schema-id in local cache → cache hit!
  2. Message: [0x00][0x00 0x00 0x00 0x01][binary data]
  (no HTTP request to Schema Registry needed)

Consumer (deserialization):
  1. Reads prefix → schema-id=1
  2. KafkaAvroDeserializer → HTTP GET /schemas/ids/1
  3. Schema cached → deserialize binary data → OrderCreatedEvent object
```

### Schema Evolution — three modes

**Backward compatible (add field with default ✅):**

```json
{"name": "discountCode", "type": "string", "default": ""}
```

- Old consumers read new messages: new field = default value.
- Schema Registry accepts the new version.

**Incompatible (remove field without default ❌):**

```bash
curl -X POST http://localhost:8090/compatibility/subjects/11.orders.created-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "... without orderId ..."}'
# {"is_compatible":false}
```

Schema Registry rejects registration — producer will get `SchemaRegistryException`.

**Compatibility modes:**

```
BACKWARD  — new schema reads old messages (add fields with default)
FORWARD   — old schema reads new messages (remove fields with default)
FULL      — both new reads old AND old reads new (most restrictive)
NONE      — no checks (not recommended for production)
```

## How to Run

```bash
docker compose -f docker-compose-11.yml up --build
```

Verify 6 containers:

```bash
docker compose -f docker-compose-11.yml ps
# kafka, schema-registry-b11, kafka-ui
# order-service-b11, payment-service-b11, notification-service-b11
```

## How to Test

### 1. Verify Schema Registry started

```bash
curl http://localhost:8090/subjects
# [] (empty at start)
```

### 2. Create an order — schema registers automatically

```bash
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-42","itemCount":3}'
```

### 3. Check registered schemas

```bash
# All subjects
curl http://localhost:8090/subjects
# ["11.orders.created-value","11.orders.cancelled-value","11.payments.processed-value"]

# Schema details
curl http://localhost:8090/subjects/11.orders.created-value/versions/latest | python3 -m json.tool
```

**Windows (PowerShell):**

```powershell
Invoke-RestMethod http://localhost:8090/subjects
Invoke-RestMethod http://localhost:8090/subjects/11.orders.created-value/versions/latest
```

### 4. Simulate payment failure

```bash
curl -s -X POST "http://localhost:8083/api/payments/fail-next?count=1"

curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-42","itemCount":1}'
```

### 5. Check backward compatible schema evolution

```bash
# Adding new field discountCode with default "" — backward compatible
curl -s -X POST http://localhost:8090/compatibility/subjects/11.orders.created-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schema": "{\"type\":\"record\",\"name\":\"OrderCreatedEvent\",\"namespace\":\"com.kafkalab.avro\",\"fields\":[{\"name\":\"eventVersion\",\"type\":\"string\",\"default\":\"1.0\"},{\"name\":\"orderId\",\"type\":\"string\",\"default\":\"\"},{\"name\":\"userId\",\"type\":\"string\",\"default\":\"\"},{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":\"com.kafkalab.avro.OrderItem\"},\"default\":[]},{\"name\":\"totalAmount\",\"type\":\"double\",\"default\":0.0},{\"name\":\"timestamp\",\"type\":\"string\",\"default\":\"\"},{\"name\":\"discountCode\",\"type\":\"string\",\"default\":\"\"}]}"
  }'
# {"is_compatible":true}
```

### 6. Kafka UI with Schema Registry

Open: `http://localhost:8080`

- `Topics` → `11.orders.created` → messages are deserialized in JSON view.
- `Schema Registry` → see all registered subjects and versions.

### 7. View schema-id in raw message

```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic 11.orders.created \
  --from-beginning \
  --max-messages 1 \
  --property print.headers=false
# First bytes: 0x00 + 4-byte schema-id (binary, not human-readable)
```

## How It Works Internally

### Producer — order-service

```yaml
# order-service/application.yml — branch11
spring:
  kafka:
    bootstrap-servers: ${SPRING_KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    properties:
      schema.registry.url: ${SCHEMA_REGISTRY_URL:http://localhost:8090}
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: io.confluent.kafka.serializers.KafkaAvroSerializer
```

```kotlin
// OrderService.kt — uses generated Avro class
val event = OrderCreatedEvent.newBuilder()
    .setEventVersion("1.0")
    .setOrderId(UUID.randomUUID().toString())
    .setUserId(userId)
    .setItems(items)
    .setTotalAmount(totalAmount)
    .setTimestamp(LocalDateTime.now().toString())
    .build()

kafkaTemplate.send("11.orders.created", userId, event)
```

### Consumer — payment-service / notification-service

```yaml
# payment-service/application.yml — consumer
spring:
  kafka:
    properties:
      schema.registry.url: ${SCHEMA_REGISTRY_URL:http://localhost:8090}
    consumer:
      value-deserializer: io.confluent.kafka.serializers.KafkaAvroDeserializer
      properties:
        specific.avro.reader: true  # ← uses the concrete generated class
```

```kotlin
// OrderPaymentListener.kt
@KafkaListener(topics = ["11.orders.created"], groupId = "payment-service-group")
fun handleOrderCreated(record: ConsumerRecord<String, OrderCreatedEvent>) {
    val event = record.value()    // ← generated Avro class, not a Kotlin data class
    log.info("[PAYMENT] Order {} from {} → amount={}", event.orderId, event.userId, event.totalAmount)
}
```

## Project Structure (changes from branch10)

```
kafka-laboratory/
├── docker-compose-11.yml                              ← +schema-registry-b11 :8090
├── branch11_schema_registry/
│   ├── order-service/
│   │   ├── src/main/avro/                             ← NEW: .avsc files
│   │   │   ├── OrderCreatedEvent.avsc
│   │   │   ├── OrderCancelledEvent.avsc
│   │   │   └── OrderItem.avsc
│   │   └── build.gradle.kts                           ← +avro plugin, +confluent serializer
│   ├── payment-service/
│   │   ├── src/main/avro/
│   │   │   ├── OrderCreatedEvent.avsc                 ← copy of the schema
│   │   │   └── PaymentProcessedEvent.avsc
│   │   └── src/main/resources/application.yml        ← KafkaAvroSerializer/Deserializer
│   └── notification-service/
│       ├── src/main/avro/
│       │   ├── OrderCreatedEvent.avsc
│       │   ├── OrderCancelledEvent.avsc
│       │   └── PaymentProcessedEvent.avsc
│       └── src/main/resources/application.yml        ← specific.avro.reader: true
└── README11.md
```

## What's Next — branch12

- **Idempotent Producer (deeper)** — producer-id, sequence number, exactly-once.
- **Kafka Transactions** — `transaction-id-prefix`, `beginTransaction` / `abortTransaction`.
- **`read_committed`** — consumer sees only committed transactions.
- **`executeInTransaction`** — Spring Kafka API for transactional sending.
- **Abort demo** — notification-service does not receive messages from an aborted transaction.

---
---

## Presentation Slides (12 slides)

**Slide 1: Branch 11 — Schema Registry & Avro**
- Apache Kafka for Certification & Production
- Avro, Schema Registry, schema evolution

**Slide 2: Agenda**
1. JSON vs Avro — size and contract
2. .avsc schema and code generation
3. Schema Registry — registration and caching
4. schema-id in payload
5. specific.avro.reader
6. Schema evolution — Backward compatible
7. Schema evolution — Forward, Full, None
8. Incompatible change → SchemaRegistryException
9. Schema Registry REST API
10. Key takeaways & CCDAK

**Slide 3: JSON vs Avro**
- JSON: ~300 bytes, field names in every message, no contract
- Avro: ~80 bytes, field names only in the schema, strict typing
- Avro: 5-byte prefix (magic + schema-id) before binary payload
- Avro: Schema Registry rejects incompatible schemas before publish

**Slide 4: .avsc Schema**
- JSON file that describes the structure of a record
- `type: "record"`, fields with name and type
- Default values → backward compatible evolution
- Gradle Avro Plugin generates Java classes: `./gradlew generateAvroJava`

**Slide 5: Schema Registry — How It Works**
- HTTP service for storing and versioning schemas
- First publish: producer POSTs schema → receives schema-id
- Subsequent publishes: schema-id from cache → no HTTP request
- Deserialization: schema-id from payload → GET schema → deserialize

**Slide 6: schema-id in Payload**
- Every Avro message: `[0x00][4 bytes schema-id][binary avro data]`
- Magic byte `0x00` — Confluent wire format marker
- Consumer always knows which schema to use
- No need for `__TypeId__` header (unlike JSON)

**Slide 7: specific.avro.reader**
- `specific.avro.reader: true` — deserializer uses the generated class
- `specific.avro.reader: false` — deserialization into `GenericRecord` (dynamic)
- `SpecificRecord` — statically typed, IntelliJ autocomplete
- `GenericRecord` — dynamic, suitable for generic pipelines

**Slide 8: Schema Evolution — Backward**
- Backward compatible: new schema reads old messages
- Add field with `default` ← always backward compatible
- Remove field without `default` ← backward incompatible
- Change field type ← always incompatible

**Slide 9: Compatibility Modes**
- BACKWARD: new reads old messages (most common)
- FORWARD: old reads new messages
- FULL: new reads old AND old reads new
- NONE: no checks (not for production)

**Slide 10: Incompatible Change**
- Removing a required field → Schema Registry rejects
- `{"error_code": 409, "message": "Schema incompatible..."}`
- Producer cannot publish a message with an incompatible schema
- This is a safety net before deployment

**Slide 11: Key Takeaways**
- Avro + Schema Registry — production standard for Confluent platform
- Schema evolution through `default` values — mandatory practice
- schema-id in payload = no ClassNotFoundException problem
- Schema Registry — compile-time + runtime contract between services
- Avro ~3.5x smaller than JSON — significant at high volumes

**Slide 12: What's Next — Branch 12: Idempotent Producer & Transactions**
- Idempotent producer — producer-id, sequence numbers
- Kafka Transactions — atomically publish to multiple topics
- read_committed — consumer sees only committed messages
- Exactly-once semantics (EOS)

## Presentation Script

**Slide 1:**
Branch eleven — Schema Registry and Avro.
After JSON serialization we move to a more powerful solution.
Avro and Schema Registry solve problems of contract, size, and compatibility between services.

**Slide 2:**
We will cover ten topics from format comparison to the Schema Registry REST API.
Key insight: Schema Registry is not just an optimization, it is an architectural governance tool.

**Slide 3:**
JSON is convenient but inefficient: field names in every message take up space.
Avro stores field names only in the schema in Schema Registry.
Every message is a 5-byte prefix and pure binary data.
Result: 3–4 times smaller size and strict typing.

**Slide 4:**
avsc is a JSON file that describes the structure of an event.
Gradle Avro Plugin automatically generates Java classes from these files.
Default values in fields — the key to backward compatible evolution.
Without a default — the field is required, and removing it will break compatibility.

**Slide 5:**
Schema Registry works as an HTTP service.
On first publish the producer registers the schema and receives a numeric schema-id.
On subsequent publishes — schema-id is taken from the local cache.
During deserialization the consumer reads schema-id from the payload and fetches the schema.

**Slide 6:**
Confluent Wire Format: magic byte zero, four bytes schema-id, then Avro binary.
This prefix is in every message.
Consumer always knows which schema-id to use — no class type problem.
This fundamentally differs from JSON where a TypeId header is needed.

**Slide 7:**
specific.avro.reader selects the deserialization mode.
Specific: uses the generated class — safe, with compile-time checks.
Generic: returns GenericRecord — flexible but with runtime access via get().
For most production use cases specific is the better choice.

**Slide 8:**
Backward compatibility — the most important mode.
The new schema version must be able to read old messages still in the topic.
Rule: if you add a field — always specify a default.
Without a default — new consumers cannot read old messages.

**Slide 9:**
The four compatibility modes cover different scenarios.
BACKWARD for most cases: rolling upgrade consumers before producers.
FORWARD for rolling upgrade producers before consumers.
FULL for bidirectional compatibility.
NONE only for development or internal topics.

**Slide 10:**
Incompatible changes are blocked at the Schema Registry level.
Producer cannot even publish a message with a schema that violates the contract.
This protects against the "works in dev, breaks in prod" situation.
Much better than ClassNotFoundException at runtime.

**Slide 11:**
Key takeaways: Avro is the production standard for Confluent Kafka.
Schema evolution through default values — a required practice.
Schema Registry as a governance tool: schema changes must respect the contract.
Smaller message size significantly reduces costs at high volumes.

**Slide 12:**
The next branch — Idempotent Producer and Transactions.
This is the culmination of the producer topic: exactly-once semantics.
Transactions allow atomic publishing to multiple topics.
read_committed consumer sees only committed messages.

## Test Questions (up to 10 questions)

**Question 1:**
What is the role of Schema Registry in the Kafka ecosystem?

A) Caching Kafka messages for faster reading
B) Storing, versioning, and validating serialization schemas
C) Managing consumer groups and their offsets
D) Load balancing between brokers

**Answer:** B —
Schema Registry — an HTTP service for centralized schema management.
Producers register schemas; consumers fetch them.
Compatibility checks block breaking changes before deployment.

---

**Question 2:**
An Avro message begins with bytes `[0x00][0x00 0x00 0x00 0x01]`.
What do these 5 bytes mean?

A) Magic byte (Confluent Wire Format) + 4-byte schema-id=1
B) Avro magic number + record length
C) Schema version + timestamp
D) Compression codec + checksum

**Answer:** A —
`0x00` — Confluent magic byte (Wire Format marker).
The next 4 bytes — schema-id (big-endian int32).
`0x00 0x00 0x00 0x01` = schema-id = 1.

---

**Question 3:**
A developer wants to add a new field `discountCode: String` to `OrderCreatedEvent`.
What condition ensures a backward compatible change?

A) Specify `"default": ""` in the .avsc file
B) Specify the field type as `"type": "null"`
C) Remove the `timestamp` field before adding the new one
D) Increment `eventVersion` in all existing consumers

**Answer:** A —
Backward compatible: new schema reads old messages.
Old messages do not have `discountCode` → a default is needed to fill it in.
Without default → Schema Registry rejects as incompatible.

---

**Question 4:**
A consumer with `specific.avro.reader: true` receives a message.
What advantage does this give over `specific.avro.reader: false`?

A) Smaller message size
B) Statically typed class with compile-time checks vs dynamic GenericRecord
C) Automatic schema compatibility check
D) Ability to read messages without Schema Registry

**Answer:** B —
`specific.avro.reader: true` → generated class (SpecificRecord) with static methods.
`specific.avro.reader: false` → GenericRecord → `record.get("fieldName")` at runtime.
Specific is safer and more convenient for typed code.

---

**Question 5:**
What is the difference between BACKWARD and FORWARD compatibility in Schema Registry?

A) BACKWARD: new schema reads old messages; FORWARD: old reads new
B) BACKWARD: old schema reads new messages; FORWARD: new reads old
C) BACKWARD and FORWARD are different names for the same concept
D) BACKWARD for producer; FORWARD for consumer

**Answer:** A —
BACKWARD: consumers can be upgraded before producers (new schema is backward-compatible).
FORWARD: producers can be upgraded before consumers (old schema reads new messages).

---

**Question 6:**
A producer tries to publish a message with a schema that violates BACKWARD compatibility.
What will happen?

A) Message will be published with a warning in logs
B) Schema Registry will return an error; producer cannot register the schema
C) The Kafka broker will reject the message
D) Consumer groups will receive an alert

**Answer:** B —
Schema Registry checks compatibility when registering a new version.
Incompatible schema → HTTP 409 Conflict → `SchemaRegistryException` in the producer.
The message will not be published.

---

**Question 7:**
What is the approximate size difference between JSON and Avro for a typical OrderCreatedEvent (~8 fields)?

A) Avro is twice as large — more metadata
B) Roughly the same
C) Avro is approximately 3–4 times smaller
D) Avro is smaller only for large objects (>1KB)

**Answer:** C —
JSON ~300 bytes (field names in every message).
Avro ~80 bytes (5-byte prefix + binary data without field names).
~3.5x difference is significant at millions of messages.

---

**Question 8:**
What is the purpose of the Gradle Avro Plugin in the project?

A) Automatically publishing schemas to Schema Registry during build
B) Generating Java classes from `.avsc` files during `./gradlew build`
C) Checking compatibility between Avro versions
D) Converting JSON messages to Avro format

**Answer:** B —
Avro Plugin reads `.avsc` files in `src/main/avro/` and generates Java classes.
These classes end up in `build/generated-main-avro-java/`.
Developers only edit `.avsc` schemas — not the generated classes.

---

**Question 9:**
Why does a producer not need to depend on the consumer's classpath with Avro?

A) Avro does not support dependencies between services
B) schema-id in payload → consumer fetches schema from Registry → no classpath dependency
C) Confluent serializer automatically converts types
D) Avro schemas are compiled into a shared JAR

**Answer:** B —
Producer writes schema-id (a number) in the payload.
Consumer fetches the schema from Registry by this ID.
Services have their own copies of schemas in their `.avsc` files — loose coupling.
No need for a shared library between services.

---

**Question 10:**
A company wants the strictest possible check: new code reads old messages AND old code reads new messages.
Which compatibility mode should be chosen?

A) BACKWARD
B) FORWARD
C) FULL
D) NONE

**Answer:** C —
FULL = BACKWARD + FORWARD simultaneously.
The most restrictive mode — more rules for developers.
Suitable for critical systems where any breaking change is unacceptable.