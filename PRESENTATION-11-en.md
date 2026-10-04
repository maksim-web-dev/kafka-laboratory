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

# Branch 11
# Schema Registry & Avro

**Apache Kafka for Certification & Production**

---

## Agenda

1. JSON vs Avro — size and contract comparison
2. `.avsc` schema and code generation
3. Schema Registry — registration and caching
4. `schema-id` in payload — Confluent wire format
5. `specific.avro.reader` vs `GenericRecord`
6. Schema evolution — BACKWARD compatible
7. Compatibility modes: BACKWARD, FORWARD, FULL, NONE
8. Incompatible change → `SchemaRegistryException`
9. Schema Registry REST API
10. Key Takeaways & What's Next

---

## JSON vs Avro

**Size and contract comparison:**

```
JSON message (~300 bytes):
{
  "orderId": "550e8400-e29b-41d4-a716-446655440000",
  "userId":  "user-42",
  "totalAmount": 149.99,
  "status": "CREATED"
}
Field names repeated in EVERY message — no schema enforcement

Avro message (~80 bytes):
  [0x00][4 bytes: schema-id][binary Avro data]
Field names stored ONLY in schema (in Schema Registry)
Schema enforced at both write and read time
```

<br>

> **3.5× smaller** at scale = significant storage and network savings.
> Schema Registry adds compile-time + runtime contract enforcement.

---

## .avsc Schema

**JSON file describing the record structure:**

```json
{
  "type": "record",
  "name": "OrderCreatedEvent",
  "namespace": "com.kafkalab.avro",
  "fields": [
    {"name": "orderId",     "type": "string"},
    {"name": "userId",      "type": "string"},
    {"name": "totalAmount", "type": "double"},
    {"name": "status",      "type": "string",  "default": "CREATED"},
    {"name": "eventVersion","type": "string",  "default": "1.0"}
  ]
}
```

**Gradle Avro Plugin generates Java/Kotlin classes:**

```bash
./gradlew generateAvroJava
# → generates: build/generated-main-avro-java/com/kafkalab/avro/OrderCreatedEvent.java
```

> **`default` value** = field is optional for old writers → backward compatible.

---

## Schema Registry — How It Works

**HTTP service for schema storage and versioning:**

```
First publish (producer startup):
  producer → POST /subjects/orders.created-value/versions
  Schema Registry validates → assigns schema-id=5
  producer caches: schema-id=5

Subsequent messages:
  producer uses cached schema-id=5 → no HTTP request

Deserialization (consumer):
  reads 4-byte schema-id from payload
  → GET /schemas/ids/5 (cached after first call)
  → deserialize binary data using that schema
```

<br>

- Schema registered once → cached in both producer and consumer
- Consumer always knows which schema to use — no `__TypeId__` header needed

---

## schema-id in Payload — Confluent Wire Format

**Every Avro message begins with a 5-byte prefix:**

```
Byte layout:
  [0x00]           ← magic byte (Confluent wire format marker)
  [00 00 00 05]    ← schema-id (4 bytes, big-endian) = schema #5
  [binary Avro]    ← actual data

Example:
  0x00 0x00 0x00 0x00 0x05 <avro-binary-data...>
```

<br>

- Consumer reads schema-id → fetches schema from registry → deserializes
- No need for `__TypeId__` header (unlike JSON)
- Schema-id = stable reference across all producers and consumers

> This is why Avro eliminates the `ClassNotFoundException` problem entirely.

---

## specific.avro.reader

**Two deserialization modes:**

```
specific.avro.reader: true  (recommended for typed services)
  Deserializes into generated class: OrderCreatedEvent
  → compile-time type safety
  → IntelliJ autocomplete works
  → type mismatch caught at build time

specific.avro.reader: false  (for generic pipelines)
  Deserializes into GenericRecord
  → access fields as: record.get("orderId").toString()
  → dynamic but no compile-time safety
  → useful for schema-agnostic processors (routers, loggers)
```

> For most production microservices: **`specific.avro.reader: true`**

---

## Schema Evolution — BACKWARD Compatible

**BACKWARD = new schema can read messages written with OLD schema**

```
Version 1 (current):
  fields: orderId, userId, totalAmount

Version 2 (new — adding a field):
  fields: orderId, userId, totalAmount, category (default: "general")

Schema Registry with BACKWARD mode:
  ✓ Version 2 reads old V1 messages (category → default "general")
  ✓ Version 2 reads new V2 messages
  This is valid: new schema can always read old data
```

**Breaking changes (BACKWARD incompatible):**
- Remove a field that has **no default** → old messages missing the field
- Change field type (e.g., `string` → `int`)

> Rule: **always add `default` when adding new fields**.

---

## Compatibility Modes

**Four modes for different deployment strategies:**

```
BACKWARD (most common):
  New schema reads OLD messages
  → rolling upgrade: update consumers BEFORE producers

FORWARD:
  Old schema reads NEW messages
  → rolling upgrade: update producers BEFORE consumers

FULL:
  Both: new reads old AND old reads new
  → most restrictive, maximum safety

NONE:
  No compatibility checks
  → development / internal topics only
```

> Production standard: **BACKWARD** or **FULL** for shared topics.

---

## Incompatible Change → SchemaRegistryException

**Schema Registry blocks incompatible changes BEFORE deployment:**

```
Attempting to publish with schema that removes required field:

Error response from Schema Registry:
{
  "error_code": 409,
  "message": "Schema being registered is incompatible with an
              earlier schema for subject orders.created-value"
}

Producer CANNOT publish → deployment blocked ✓
```

<br>

**This is the safety net:**
- Incompatible schema change → rejected at registration time
- NOT discovered in production via `ClassNotFoundException`
- Much better than runtime failure with user impact

> Schema Registry = governance: breaking changes are blocked, not discovered.

---

## Key Takeaways

- **Avro + Schema Registry** = production standard for Confluent platform
- **~3.5× smaller** than JSON — significant savings at scale
- **`schema-id` in payload** = no `ClassNotFoundException` problem
- **`default` values** in fields = the only way to safely add new fields
- **BACKWARD** compatibility = always add defaults, never remove required fields
- **`specific.avro.reader: true`** = type-safe, compile-time checked
- **Schema Registry blocks** incompatible changes before they reach production

<br>

> Schema Registry is not just optimization — it's architectural governance.

---

## What's Next — Branch 12

**Idempotent Producer & Transactions — Exactly-Once Semantics**

- Idempotent producer: `producer-id` + `sequence number` → broker skips duplicates
- Kafka Transactions: atomic publish to multiple topics
- `read_committed` — consumer sees only committed messages
- `executeInTransaction` — Spring Kafka transaction API
- Zombie fencing — prevents split-brain in distributed systems

---

<!-- _class: title -->

## What's Next — Branch 12
- Idempotent producer: no duplicates on retry
- Transactions: atomic multi-topic publish
- read_committed: only committed messages visible
- Zombie fencing: one producer at a time

# Questions?

**Branch 11 — Schema Registry & Avro**