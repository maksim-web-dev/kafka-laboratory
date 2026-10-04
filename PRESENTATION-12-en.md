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

# Branch 12
# Idempotent Producer & Transactions

**Apache Kafka for Certification & Production**

---

## Agenda

1. The duplicate problem without idempotence
2. Idempotent Producer — `producer-id` + sequence numbers
3. Transactions — why they're needed beyond idempotence
4. `transactional-id-prefix` and recovery on restart
5. `executeInTransaction` — Spring Kafka API
6. Commit flow — normal path
7. Abort flow — exception path
8. Control batches in the topic
9. `read_committed` vs `read_uncommitted`
10. Zombie fencing
11. Key Takeaways & What's Next

---

## The Duplicate Problem

**Without idempotence: timeout + retry = duplicate record in Kafka**

```
Without idempotence (acks=1, retries=3):

  t=0ms  producer.send(payment-event)
  t=50ms broker receives, writes to log
  t=5000ms network timeout — NO ACK received by producer
  t=5001ms producer retries (automatic)
  t=5050ms broker receives AGAIN → writes SECOND copy

  Result: payment processed TWICE → critical billing error
```

<br>

- This is a real risk in any distributed system with retries
- NOT solved by `acks=all` alone
- Requires **idempotent producer** configuration

> Duplicates from retries are silent and hard to detect without idempotence.

---

## Idempotent Producer

**`enable.idempotence=true` — broker-side deduplication:**

```
Startup: Kafka assigns producer-id=42 to this producer instance

Message 1 sent: producer-id=42, partition=0, seq=0
  → broker writes record (seq=0 not seen before) ✓

Message 1 retried: producer-id=42, partition=0, seq=0
  → broker: seq=0 already written for producer-id=42
  → SILENTLY SKIPPED — no duplicate ✓

Requirements:
  acks=all                                    (mandatory)
  retries > 0                                 (mandatory)
  max.in.flight.requests.per.connection ≤ 5  (mandatory)
```

> In Kafka 3.0+: `enable.idempotence=true` is the default with `acks=all`.

---

## Transactions — Beyond Idempotence

**Idempotence protects ONE publish. Transactions protect MULTIPLE publishes atomically:**

```
Scenario: payment-service must:
  1. publish PaymentProcessedEvent to 12.payments.processed
  2. publish AuditEvent to 12.audit.events

Without transactions:
  Step 1 succeeds → PaymentProcessedEvent visible to consumers
  Step 2 fails    → AuditEvent never written
  Result: inconsistent state — payment without audit record

With transactions:
  Both steps are atomic:
    either BOTH committed → both visible
    or NEITHER committed → neither visible
```

> Transactions = atomic multi-topic publish.

---

## transactional-id-prefix

**Stable identity for transaction recovery on restart:**

```yaml
spring:
  kafka:
    producer:
      transaction-id-prefix: payment-service-
```

```
Kafka generates transactional.id per partition:
  payment-service-0   (for partition 0)
  payment-service-1   (for partition 1)

On producer restart:
  New instance registers same transactional.id
  Kafka finds open (uncommitted) transaction from previous instance
  → automatically ABORTS the incomplete transaction
  → starts fresh

Guarantees: no orphaned open transactions after crash
```

---

## executeInTransaction — Spring Kafka API

**Wraps business logic in a Kafka transaction:**

```kotlin
@KafkaListener(topics = ["12.orders.created"])
fun handleOrderCreated(event: OrderCreatedEvent) {
    kafkaTemplate.executeInTransaction { ops ->
        val result = paymentService.processPayment(event)
        ops.send(
            "12.payments.processed",
            event.userId,
            PaymentProcessedEvent(event.orderId, result.status)
        )
        // RuntimeException here → automatic abortTransaction()
    }
    // On success → automatic commitTransaction()
}
```

<br>

- `beginTransaction()` called automatically before the lambda
- `commitTransaction()` on successful return
- `abortTransaction()` on any `RuntimeException`

---

## Commit Flow — Normal Path

**All messages visible to `read_committed` consumers:**

```
order-service:   publish OrderCreatedEvent (idempotent, no transaction needed)
                        │
                        ▼
payment-service: executeInTransaction {
                   processPayment() → APPROVED
                   send(PaymentProcessedEvent(APPROVED))
                 }  → commitTransaction()
                        │
                        ▼ COMMIT control batch written to topic
notification:    reads PaymentProcessedEvent(APPROVED) ✓
                 (read_committed waits for commit before delivering)
```

---

## Abort Flow — Exception Path

**Aborted messages invisible to `read_committed` consumers:**

```
order-service:   publish OrderCreatedEvent
                        │
                        ▼
payment-service: executeInTransaction {
                   processPayment() → throws RuntimeException
                 }  → abortTransaction() (automatic)
                        │
                        ▼ ABORT control batch written to topic
notification:    does NOT see PaymentProcessedEvent ✓
                 (read_committed filters out ABORTED records)

Kafka UI shows:  transaction marker ABORT in topic
```

> ABORT marker is physically written to the topic — consumers skip it.

---

## read_committed vs read_uncommitted

**Consumer isolation level:**

```
read_uncommitted (default):
  Consumer reads ALL records immediately
  Including records from OPEN (uncommitted) transactions
  Highest throughput — no waiting
  Risk: may read records that are later ABORTED

read_committed:
  Consumer reads ONLY records from COMMITTED transactions
  Waits until transaction is closed (commit or abort)
  Consumer LAG may grow if transaction stays open too long
  Mandatory for EOS (Exactly-Once Semantics) guarantees
```

```yaml
spring.kafka.consumer.isolation-level: read_committed
```

> `read_committed` is required for EOS. Without it, transactions don't fully protect consumers.

---

## Zombie Fencing

**Protection against split-brain with same transactional.id:**

```
Scenario:
  Old producer instance (zombie) was slow/paused
  New producer instance starts with same transactional.id

Without fencing:
  Both old and new publish → duplicate records, inconsistent state

With zombie fencing:
  New instance registers transactional.id with higher epoch
  Old instance (zombie) sends next record → broker rejects:
    ProducerFencedException: "A newer producer with the same
    transactional.id has been registered"
  Zombie is forced to stop immediately

Guarantee: at most ONE active producer per transactional.id
```

---

## Key Takeaways

- **EOS** = idempotent producer + transactions + `read_committed` — all three required
- **Idempotence** prevents broker-level duplicates on retry
- **Transactions** = atomic publish to multiple topics (all-or-nothing)
- **`transactional-id-prefix`** = automatic rollback of incomplete transactions on restart
- **`read_committed`** = consumer sees only committed messages (mandatory for EOS)
- **Zombie fencing** = only one active producer per `transactional.id`
- **EOS has higher latency** — use only where duplicates are truly unacceptable

<br>

> Exactly-Once Semantics = idempotence + transactions + read_committed, working together.

---

## What's Next — Branch 13

**Saga Pattern (Choreography) — distributed transactions without 2PC**

- 4 services, 8 Kafka topics for one business process
- Choreography: each service reacts to events, publishes next step
- Compensating transactions: rollback via business events
- Idempotency key: protect against duplicate processing in at-least-once

---

<!-- _class: title -->

## What's Next — Branch 13
- Saga: distributed transactions without 2PC
- Choreography: no central coordinator
- Compensation events on failure
- Idempotency key: deduplicate at business logic level

# Questions?

**Branch 12 — Idempotent Producer & Transactions**