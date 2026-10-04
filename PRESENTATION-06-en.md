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

# Branch 06
# Offsets & Commits

**Apache Kafka for Certification & Production**

---

## Agenda

1. The auto-commit problem — at-most-once risk
2. Manual commit — at-least-once guarantee
3. `enable.auto.commit: false` + `ack-mode: MANUAL_IMMEDIATE`
4. `ack.acknowledge()` — explicit offset commit
5. `ack.nack(Duration)` — reject with redelivery
6. Idempotency requirement
7. Demo: simulate-failure → redelivery
8. `MANUAL_IMMEDIATE` vs `MANUAL`
9. Delivery strategies: at-most-once vs at-least-once vs exactly-once
10. Key Takeaways & What's Next

---

## The Auto-Commit Problem

**`enable.auto.commit=true` (default): offset committed every 5 seconds**

```
Timeline with auto-commit:

t=0s   consumer reads message
t=5s   Kafka auto-commits offset  ← regardless of processing state
t=6s   service crashes during processing

Result: offset committed, message LOST — never redelivered
```

<br>

- Kafka commits before processing is confirmed
- If service crashes between auto-commit and completion → **message lost forever**
- This is **at-most-once**: each message processed **at most** once

> Auto-commit is convenient but dangerous for critical business events.

---

## Manual Commit — At-Least-Once

**Commit offset ONLY after successful processing:**

```
Timeline with manual commit:

t=0s   consumer reads message
t=1s   business logic executes successfully
t=1s   ack.acknowledge() → offset committed now

t=0s   consumer reads message
t=1s   service crashes BEFORE acknowledge()
→ offset NOT committed
→ on restart: Kafka redelivers the same message ✓
```

<br>

- **at-least-once**: each message processed **at least** once
- Duplicates are possible — requires **idempotent** handler
- Config: `enable.auto.commit: false` + `ack-mode: MANUAL_IMMEDIATE`

> Process first, commit after. This is the production standard.

---

## Acknowledgment API

**Spring Kafka passes `Acknowledgment` as a listener parameter:**

```kotlin
@KafkaListener(topics = ["06.orders.created"])
fun handleOrderCreated(
    event: OrderCreatedEvent,
    ack: Acknowledgment       // ← injected by Spring Kafka
) {
    try {
        processOrder(event)   // business logic
        ack.acknowledge()     // ← commit offset immediately
    } catch (e: Exception) {
        ack.nack(Duration.ofSeconds(2))  // ← retry after 2s
    }
}
```

<br>

- `ack.acknowledge()` — commits offset (MANUAL_IMMEDIATE = immediately)
- `ack.nack(Duration)` — rejects message, retry after delay
- While nack is pending: **next messages in partition are NOT read**

---

## Demo — Simulate Failure

**Plan N consecutive failures via REST:**

```bash
# Plan 2 failures for the next message
curl -X POST http://localhost:8082/api/notifications/simulate-failure/2

# Send an order
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-01","product":"Book","quantity":1,"totalAmount":20.00}'
```

**Expected log sequence:**

```
[ATTEMPT 1] Processing order ... → nack (simulated failure)
[RETRY 1]   After 2s: processing order ... → nack (simulated failure)
[RETRY 2]   After 2s: processing order ... → acknowledge ✓
```

**Stats endpoint:** `GET /api/notifications/count`
→ `{"ordersCreated": 1, "redelivered": 2}`

---

## Idempotency Requirement

**At-least-once means duplicates are normal, not exceptional**

```kotlin
// NON-idempotent handler — DANGEROUS with at-least-once:
fun handleOrder(event: OrderCreatedEvent) {
    db.insert(order)   // duplicate orderId → constraint violation!
    emailService.send()   // sends email twice!
    ack.acknowledge()
}

// IDEMPOTENT handler — safe:
fun handleOrder(event: OrderCreatedEvent) {
    if (db.existsByOrderId(event.orderId)) return  // skip duplicate
    db.insert(order)
    emailService.sendIfNotSent(event.orderId)
    ack.acknowledge()
}
```

> If business logic cannot be made idempotent → use **exactly-once** (branch12).

---

## MANUAL_IMMEDIATE vs MANUAL

**Two manual commit modes in Spring Kafka:**

```
MANUAL_IMMEDIATE (recommended for most cases):
  ack.acknowledge() → commit happens immediately in this call
  Simple, predictable, low latency

MANUAL (batch scenarios):
  ack.acknowledge() → commit deferred until next poll()
  Useful for batch processing optimizations
  Risk: un-committed offsets if crash between acknowledge() and poll()

BATCH:
  One ack for the entire batch at once
  Highest throughput, lowest granularity
```

> For most microservices: **`MANUAL_IMMEDIATE`** is simpler and safer.

---

## Delivery Strategies Comparison

| Strategy | Config | Guarantees | Use case |
|----------|--------|-----------|---------|
| **at-most-once** | auto-commit | No loss guarantee | Metrics, logs |
| **at-least-once** | manual commit | No message loss | Business events |
| **exactly-once** | transactions | No loss, no dups | Finance, payments |

<br>

```
at-most-once:  simple implementation, acceptable loss
at-least-once: idempotent handler required, possible duplicate
exactly-once:  Kafka transactions (branch12), highest complexity
```

> Choose based on **business requirements**, not technical preference.
> Most microservices: **at-least-once** with idempotent handlers.

---

## Key Takeaways

- **`enable.auto.commit=false`** + **`ack-mode=MANUAL_IMMEDIATE`** = production standard
- **`ack.acknowledge()`** after successful processing = at-least-once guarantee
- **`ack.nack(Duration)`** = controlled retry without message loss
- **Idempotency** is mandatory when using manual commit
- **Duplicates are normal** in at-least-once systems — design for them
- **MANUAL_IMMEDIATE** simpler and safer than MANUAL for typical services
- Auto-commit is fine for non-critical logs; never use it for financial events

<br>

> Process first, commit after. Always design handlers to be idempotent.

---

## What's Next — Branch 07

**Producer Configuration — reliability on the sending side**

- `acks=0` vs `acks=1` vs `acks=all` — three confirmation levels
- ISR — In-Sync Replicas and `min.insync.replicas`
- Idempotent producer — sequence numbers prevent broker-side duplicates
- `linger.ms` + `batch.size` — latency vs throughput trade-off
- `BenchmarkService` — measuring the performance difference

---

<!-- _class: title -->

## What's Next — Branch 07
- acks levels: fire-and-forget vs quorum
- Idempotent producer: no duplicates on retry
- Batching: linger.ms and batch.size
- Benchmark three configurations

# Questions?

**Branch 06 — Offsets & Commits**