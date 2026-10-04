# Branch 12 — Idempotent Producer & Transactions

## `branch12_idempotent_producer` — what we learn

- **Idempotent Producer** — `enable.idempotence=true` with producer-id and sequence numbers.
- **Exactly-Once Semantics (EOS)** — messages are either committed or rejected — no duplicates and no loss.
- **`transactional.id`** / `transaction-id-prefix` — unique identifier for the transactional producer.
- **`kafkaTemplate.executeInTransaction`** — Spring Kafka API for atomic publishing.
- **`abortTransaction`** — rollback: message is physically written but marked ABORT.
- **`read_committed`** — consumer sees only committed messages, ignores ABORT.
- **`read_uncommitted`** (default) — consumer sees all messages including uncommitted.
- **Control batch** — a service record in the topic with type COMMIT or ABORT (visible in Kafka UI).
- **Transactional recovery** — Kafka recovers incomplete transactions on producer restart.
- **`abort-next` endpoint** — simulate abort to demo exactly-once guarantees.

## What changed compared to branch11

- `order-service` — `enable.idempotence=true` (stricter configuration, no API changes).
- `payment-service` — added `transaction-id-prefix: payment-tx-` → transactional producer.
- `payment-service` — `executeInTransaction` instead of direct `kafkaTemplate.send`.
- `notification-service` — `isolation-level: read-committed` → sees only committed.
- Schema Registry and Avro remain (same serializer/deserializer as branch11).
- New endpoint `POST /api/payments/abort-next?count=N` — schedule N aborts.
- Container schema-registry-b12 (port 8090) — identical to branch11.
- Topic names changed to `12.orders.*`, `12.payments.*`.

## Architecture

```
order-service-b12 :8081
  Idempotent Producer:
  enable.idempotence=true, acks=all, retries=MAX
          │
   12.orders.created
          │
          ▼
payment-service-b12 :8083
  Transactional Producer:
  transaction-id-prefix: payment-tx-

  executeInTransaction:
    ┌─────────────────────────────────────────────┐
    │ 1. BEGIN TRANSACTION                         │
    │ 2. send(12.payments.processed, payment)      │
    │ 3. if shouldAbort: throw RuntimeException    │
    │    → abortTransaction (ABORT control batch)  │
    │    else: commitTransaction (COMMIT ctrl batch)│
    └─────────────────────────────────────────────┘
          │
   12.payments.processed
          │
          ▼
notification-service-b12 :8082
  isolation-level: read-committed
  → sees: COMMITTED messages ✓
  → ignores: ABORTED messages ✗
```

### Topics and their configuration

- `12.orders.created` → partitions: 3, retention: 7 days, Avro, idempotent producer.
- `12.orders.cancelled` → partitions: 1, retention: 7 days.
- `12.payments.processed` → partitions: 3, retention: 7 days,
  transactional producer, control batches visible in Kafka UI.

## Key Concepts of This Branch

### Idempotent Producer — deduplication on retry

Problem without idempotence:

```
Producer: send(orderId=ABC, seq=5)
  → network failure (message arrived, ACK lost)
  → timeout → Retry: send(orderId=ABC, seq=5)
  → Broker writes AGAIN → duplicate in topic ❌
```

With `enable.idempotence=true`:

```
Broker: "already have (producer-id=42, partition=1, seq=5)" → duplicate? skip ✓

Requirements (interdependent):
  acks=all                                    (required)
  retries ≥ 1                                 (required)
  max.in.flight.requests.per.connection ≤ 5   (required)
```

### Kafka Transactions — atomic publish

A transactional producer guarantees that a group of messages is either **all committed** or **all aborted**:

```kotlin
// payment-service/OrderPaymentListener.kt
kafkaTemplate.executeInTransaction { kt ->
    val payment = PaymentProcessedEvent.newBuilder()
        .setStatus("APPROVED")
        // ...
        .build()

    kt.send("12.payments.processed", event.orderId.toString(), payment)

    if (shouldAbort) {
        throw RuntimeException("Simulated abort")
        // Spring Kafka: abortTransaction() → control batch ABORT
    }
    // Spring Kafka: commitTransaction() → control batch COMMIT
}
```

### Abort flow — what happens in the topic

```
12.payments.processed topic:

Offset 0: [payment for order-A]  ← COMMITTED (control batch COMMIT)
Offset 1: [payment for order-B]  ← ABORTED (control batch ABORT)
Offset 2: [COMMIT marker]
Offset 3: [ABORT marker]

read_uncommitted consumer: sees offsets 0, 1 (and control batches)
read_committed consumer:   sees only offset 0 (ignores offset 1 + ABORT marker)
```

Control batches (COMMIT/ABORT markers) are **physically** written to the topic but `read_committed` skips them.
Visible in Kafka UI as "transaction marker" or "control batch".

### `transaction-id-prefix` and recovery

`transactional.id` (or prefix) uniquely identifies the producer instance:

```yaml
# payment-service/application.yml
spring:
  kafka:
    producer:
      transaction-id-prefix: payment-tx-
      # Kafka generates: payment-tx-0, payment-tx-1, ... per partition
```

On producer restart:

```
1. Producer registers with the same transactional.id (payment-tx-0)
2. Kafka checks for incomplete transactions for this ID
3. If found → automatically abort (zombie transactions not allowed)
4. New producer starts a clean transaction
```

### `read_committed` vs `read_uncommitted`

```yaml
# notification-service/application.yml — branch12
spring:
  kafka:
    consumer:
      isolation-level: read-committed     # ← NEW: committed only

# Default (without configuration):
#   isolation-level: read-uncommitted     # sees all, including uncommitted
```

- `read_committed`: consumer does not read messages until the transaction is closed.
  With an open transaction: consumer "pauses" before the first uncommitted offset.
  LAG will grow if the transaction is not closed.
- `read_uncommitted`: reads everything immediately — may receive messages from aborted transactions.

## How to Run

```bash
docker compose -f docker-compose-12.yml up --build
```

Verify 6 containers:

```bash
docker compose -f docker-compose-12.yml ps
# kafka, schema-registry-b12, kafka-ui
# order-service-b12, payment-service-b12, notification-service-b12
```

## How to Test

### 1. Normal flow — notification receives PaymentProcessed

```bash
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-1"}'

# notification-service logs:
# [NOTIFICATION] Payment APPROVED for order uuid → user user-1
```

### 2. Main demo scenario: transaction abort

```bash
# Step 1: Normal order (committed)
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-commit"}'

# Step 2: Schedule abort for the next transaction
curl -s -X POST "http://localhost:8083/api/payments/abort-next?count=1"
# {"abortScheduled":1}

# Step 3: Send the order that will be aborted
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-abort"}'

# payment-service logs:
# [PAYMENT-TX] Processing orderId=uuid-2 abort=true
# RuntimeException: Simulated abort → abortTransaction

# notification-service logs:
# (only the first order — the second is ABORTED, read_committed ignores it)
```

**Windows (PowerShell):**

```powershell
Invoke-RestMethod -Method POST -Uri http://localhost:8081/api/orders `
  -ContentType "application/json" -Body '{"userId":"user-commit"}'

Invoke-RestMethod -Method POST -Uri "http://localhost:8083/api/payments/abort-next?count=1"

Invoke-RestMethod -Method POST -Uri http://localhost:8081/api/orders `
  -ContentType "application/json" -Body '{"userId":"user-abort"}'
```

### 3. Check control batches in Kafka UI

Open: `http://localhost:8080`

- `Topics` → `12.payments.processed` → Messages:
  - First message: normal record (COMMITTED).
  - Second: invisible to read_committed (ABORTED).
  - Control batches: COMMIT marker + ABORT marker (service records).

### 4. Check isolation level via CLI

```bash
# read_committed — sees only committed:
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic 12.payments.processed \
  --from-beginning \
  --isolation-level read_committed

# read_uncommitted — sees everything (including ABORTED binary):
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic 12.payments.processed \
  --from-beginning \
  --isolation-level read_uncommitted
```

### 5. Schema Registry schemas

```bash
curl http://localhost:8090/subjects
# ["12.orders.created-value","12.payments.processed-value","..."]
```

## How It Works Internally

### Producer — order-service (idempotent)

```yaml
# order-service/application.yml — branch12
spring:
  kafka:
    producer:
      acks: all
      retries: 2147483647
      properties:
        enable.idempotence: true
        max.in.flight.requests.per.connection: 5
```

### Producer — payment-service (transactional)

```yaml
# payment-service/application.yml — branch12
spring:
  kafka:
    producer:
      acks: all
      retries: 2147483647
      properties:
        enable.idempotence: true
      transaction-id-prefix: payment-tx-   # ← NEW: transactional mode
```

```kotlin
// OrderPaymentListener.kt — branch12
@KafkaListener(topics = ["12.orders.created"], groupId = "payment-service-group")
fun handleOrderCreated(record: ConsumerRecord<String, OrderCreatedEvent>) {
    val event = record.value()
    val shouldAbort = abortNextN.get() > 0

    kafkaTemplate.executeInTransaction { kt ->
        val payment = PaymentProcessedEvent.newBuilder()
            .setStatus("APPROVED")
            .setOrderId(event.orderId.toString())
            .setUserId(event.userId.toString())
            .setAmount(event.totalAmount)
            .build()

        kt.send("12.payments.processed", event.orderId.toString(), payment)

        if (shouldAbort) {
            abortNextN.decrementAndGet()
            throw RuntimeException("Simulated abort for orderId=${event.orderId}")
            // Spring Kafka: automatically calls abortTransaction()
        }
        // Spring Kafka: automatically calls commitTransaction()
    }
}
```

### Consumer — notification-service (read_committed)

```yaml
# notification-service/application.yml — branch12
spring:
  kafka:
    consumer:
      isolation-level: read-committed     # ← NEW
```

## Project Structure (changes from branch11)

```
kafka-laboratory/
├── docker-compose-12.yml                              ← schema-registry-b12 :8090
├── branch12_idempotent_producer/
│   ├── order-service/
│   │   └── src/main/resources/application.yml        ← enable.idempotence (already in b07)
│   ├── payment-service/
│   │   └── src/main/resources/application.yml        ← +transaction-id-prefix: payment-tx-
│   │   └── src/main/kotlin/.../listener/
│   │       └── OrderPaymentListener.kt               ← executeInTransaction + abort logic
│   │   └── src/main/kotlin/.../controller/
│   │       └── PaymentController.kt                  ← +abort-next endpoint
│   └── notification-service/
│       └── src/main/resources/application.yml        ← +isolation-level: read-committed
└── README12.md
```

## What's Next — branch13

- **Saga Pattern** — distributed transactions through a sequence of Kafka events.
- **Choreography Saga** — each service responds to events and publishes the next one.
- **Compensation events** — `OrderCancelledEvent` on failure of any step.
- **Saga state machine** — tracking saga state across multiple topics.
- **Idempotency in Saga** — protection from duplicates with at-least-once delivery.

---
---

## Presentation Slides (12 slides)

**Slide 1: Branch 12 — Idempotent Producer & Transactions**
- Apache Kafka for Certification & Production
- EOS, transactions, read_committed

**Slide 2: Agenda**
1. The duplicate problem without idempotence
2. Idempotent producer — producer-id + sequence
3. Transactions — why they are needed
4. transaction-id-prefix and recovery
5. executeInTransaction
6. commitTransaction vs abortTransaction
7. Control batches in the topic
8. read_committed vs read_uncommitted
9. Zombie fencing
10. Key takeaways & CCDAK

**Slide 3: The Duplicate Problem**
- acks=1, retries=3: timeout → retry → broker writes TWICE
- Payment charged twice — critical error
- Idempotence: producer-id + sequence-number → broker skips duplicate
- Requirements: acks=all + retries>0 + max.in.flight≤5

**Slide 4: Transactional Producer**
- Idempotence: protection against duplicates on retry (single publish)
- Transactions: atomic publish to MULTIPLE topics
- transaction-id-prefix → Kafka generates transactional.id per partition
- Recovery on restart: abort incomplete transactions

**Slide 5: executeInTransaction**
- Spring Kafka API for transactional sending
- beginTransaction() automatically
- commitTransaction() or abortTransaction() automatically
- RuntimeException inside lambda → abortTransaction

**Slide 6: Normal Flow (COMMIT)**
- order-service: publish OrderCreatedEvent (idempotent)
- payment-service: executeInTransaction { send PaymentProcessedEvent }
- commitTransaction → COMMIT control batch in topic
- notification-service: sees PaymentProcessedEvent ✓

**Slide 7: Abort Flow**
- order-service: publish OrderCreatedEvent (idempotent)
- payment-service: executeInTransaction { send PaymentProcessedEvent → throw Exception }
- abortTransaction → ABORT control batch in topic
- notification-service: does NOT see PaymentProcessedEvent (read_committed) ✗

**Slide 8: Control Batches**
- COMMIT/ABORT markers are physically written to the topic
- Kafka UI shows them as "transaction marker"
- read_committed: ignores ABORTED messages and ABORT markers
- read_uncommitted: sees everything including binary ABORTED data

**Slide 9: read_committed vs read_uncommitted**
- read_uncommitted (default): max throughput, sees uncommitted
- read_committed: waits for commit/abort, sees only committed
- LAG may grow if a transaction stays open too long
- Production: read_committed for critical consumers

**Slide 10: Zombie Fencing**
- Old producer instance (zombie) may compete with the new one
- Kafka fences zombie: if a new producer with the same transactional.id
  registers → old one gets ProducerFencedException
- Guarantees: only one active producer per transactional.id
- Protection from split-brain in a distributed environment

**Slide 11: Key Takeaways**
- Idempotence + Transactions = Exactly-Once Semantics (EOS) in Kafka
- EOS = atomic: either all messages committed, or none
- read_committed is mandatory for EOS consumer
- transactional.id with prefix: automatic rollback on restart
- EOS has higher latency — use only for critical flows

**Slide 12: What's Next — Branch 13: Saga Pattern**
- Distributed transactions through a sequence of Kafka events
- Choreography Saga: each service responds to the previous event
- Compensation events: rollback through business events
- Idempotency in Saga: protection from duplicates

## Presentation Script

**Slide 1:**
Branch twelve — Idempotent Producer and Transactions.
This is the culmination of the producer reliability topic.
Here we achieve exactly-once semantics: messages are either committed or rejected — no duplicates and no loss.

**Slide 2:**
We will cover ten topics from the duplicate problem to zombie fencing.
Key insight: EOS in Kafka is not a single setting, it is a system of several components.

**Slide 3:**
Without idempotence: on timeout the producer retries the send.
If the broker received the first attempt but did not have time to acknowledge — the second write becomes a duplicate.
For payments this is critical: the customer is charged twice.
Idempotence solves this through producer-id and sequence number per partition.

**Slide 4:**
Transactions — the next level after idempotence.
Idempotence protects against duplicates on retry for a single publish.
Transactions guarantee that multiple publishes to different topics are atomic.
transaction-id-prefix identifies the producer and allows recovery on restart.

**Slide 5:**
executeInTransaction — the Spring Kafka API that wraps logic in a transaction.
beginTransaction is called automatically.
If everything succeeded — commitTransaction.
If RuntimeException — abortTransaction.
Simple and safe API for transactional logic.

**Slide 6:**
Normal flow: order-service publishes an order.
payment-service begins a transaction, sends PaymentProcessedEvent, commits.
COMMIT control batch is written to the topic.
notification-service with read_committed sees the event and reacts.

**Slide 7:**
Abort flow: same but payment-service throws an exception.
Kafka automatically calls abortTransaction.
ABORT control batch is written to the topic.
notification-service does not see PaymentProcessedEvent — transaction is rolled back.

**Slide 8:**
Control batches — service records in the topic.
COMMIT and ABORT markers are physically in the Kafka log file.
They are needed so the consumer knows when a transaction has closed.
Kafka UI shows them as transaction markers — useful for debugging.

**Slide 9:**
read_committed consumer waits until the transaction is closed.
If payment-service holds an open transaction — consumer LAG will grow.
read_uncommitted sees everything immediately — higher throughput but without EOS guarantees.
For critical consumers always use read_committed.

**Slide 10:**
Zombie fencing — protection from split-brain.
If an old producer instance is still active after restart — that is a zombie.
New instance with the same transactional.id registers → Kafka fences the zombie.
Old instance gets ProducerFencedException and stops.
Guarantees that only one producer per transactional.id is ever active.

**Slide 11:**
Key takeaways: EOS = idempotence + transactions + read_committed together.
Without read_committed on the consumer, EOS does not work fully.
transactional.id with prefix — production standard.
EOS has higher latency and complexity — use only where duplicates are unacceptable.

**Slide 12:**
The next branch — Saga Pattern.
EOS guarantees atomicity within a single producer.
Saga Pattern solves distributed transactions across multiple services.
Choreography through Kafka events, compensation events on failure.

## Test Questions (up to 10 questions)

**Question 1:**
A producer with `enable.idempotence=true` sent a message (seq=10).
The broker received it but did not send an ACK in time → timeout.
Producer retries with the same seq=10. What will happen?

A) Broker writes a duplicate
B) Broker rejects the duplicate → only one message in the topic
C) Producer gets a DuplicateSequenceException
D) Kafka increments sequence to 11 automatically

**Answer:** B —
With idempotence the broker tracks `(producer-id, partition, sequence-number)`.
Duplicate seq=10 → duplicate → rejected → only one record.

---

**Question 2:**
`kafkaTemplate.executeInTransaction { ... throw RuntimeException() }`.
What will happen to the messages sent inside the lambda?

A) Messages are committed — the exception has no effect after send()
B) Transaction is aborted — ABORT control batch in the topic
C) Messages are deferred until the next successful transaction
D) RuntimeException is ignored — transaction committed

**Answer:** B —
RuntimeException inside `executeInTransaction` → Spring Kafka calls `abortTransaction()`.
ABORT control batch is written to the topic.
`read_committed` consumers will not see these messages.

---

**Question 3:**
Consumer with `isolation-level: read-committed` reads `12.payments.processed`.
payment-service started a transaction but has not committed yet (transaction open for 10 seconds).
What will happen to the consumer?

A) Consumer reads the uncommitted message
B) Consumer "pauses" before the first uncommitted offset → LAG grows
C) Consumer gets an error and stops
D) Consumer skips uncommitted messages and continues

**Answer:** B —
`read_committed`: consumer does not read messages from open transactions.
Consumer "waits" before the uncommitted offset.
LAG grows until the transaction is closed (committed or aborted).

---

**Question 4:**
What is the purpose of `transaction-id-prefix` in Spring Kafka?

A) Encrypting transactional messages
B) Unique identifier for the producer instance for recovery and zombie fencing
C) Routing messages to specific partitions
D) Increasing throughput of the transactional producer

**Answer:** B —
`transaction-id-prefix` generates a unique `transactional.id` per partition.
Kafka uses it on restart: abort incomplete transactions from the previous instance.
Also: if a new producer registers → old one (zombie) is fenced.

---

**Question 5:**
What is the difference between `read_uncommitted` and `read_committed` consumer?

A) read_uncommitted reads only committed; read_committed reads all
B) read_uncommitted (default) reads all messages; read_committed reads only committed
C) Both read the same — the difference is only in name
D) read_committed reads faster due to fewer checks

**Answer:** B —
`read_uncommitted` (default): sees all records including uncommitted and ABORTED.
`read_committed`: sees only messages from completed (committed) transactions.

---

**Question 6:**
What is "Zombie Fencing" in Kafka transactions?

A) Blocking messages from unauthorized producers
B) Mechanism preventing a zombie instance of an old producer from publishing in an active transaction
C) Deleting old transactions after retention.ms
D) Checking schema compatibility for transactional topics

**Answer:** B —
Zombie: old producer instance still active after restart.
New instance registers with the same transactional.id → Kafka fences zombie.
Zombie receives `ProducerFencedException`.
Guarantees one active producer per transactional.id.

---

**Question 7:**
Why is `acks=all` required for an idempotent producer?

A) acks=all is required for all producer types
B) Idempotence without acks=all can still cause message loss even without duplicates
C) Kafka automatically sets acks=all for idempotent producers
D) Confluent requires acks=all for licensing

**Answer:** B —
Idempotence guarantees no duplicates.
But without `acks=all` a message may only be written to the leader and lost on failover.
`acks=all` + idempotence = no duplicates + no data loss together.

---

**Question 8:**
payment-service sent `PaymentProcessedEvent` inside a transaction but did not commit.
Will notification-service (read_committed) receive the event?

A) Yes, immediately after send()
B) No — only after commitTransaction()
C) Depends on the acks configuration
D) Only if notification-service uses read_uncommitted

**Answer:** B —
`read_committed` consumer sees messages only after `commitTransaction()`.
Before commit: message is physically in the topic but invisible to `read_committed`.

---

**Question 9:**
Which statement about Exactly-Once Semantics (EOS) in Kafka is INCORRECT?

A) EOS requires an idempotent producer
B) EOS requires a transactional producer for atomic publish
C) EOS is guaranteed only if the consumer is also transactional
D) EOS requires a read_committed consumer

**Answer:** C —
The consumer does not need to be transactional for EOS.
The consumer needs the `read_committed` isolation level.
EOS = idempotent/transactional producer + read_committed consumer.

---

**Question 10:**
A control batch of type ABORT in topic `12.payments.processed`.
What do different consumers see?

A) Both (read_committed and read_uncommitted) do not see ABORTED messages
B) read_uncommitted sees binary ABORTED data; read_committed sees nothing
C) Both see the same — the control batch does not affect reading
D) ABORT control batch is deleted after 1 minute

**Answer:** B —
`read_uncommitted`: sees the physical bytes of the ABORTED message (binary, not parseable as a business event).
`read_committed`: ignores ABORTED messages and ABORT marker — sees nothing.
Physically the records remain in the log until retention.ms.