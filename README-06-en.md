# Branch 06 — Offsets & Commits

## `branch06_offsets_commits` — what we learn

- **Manual commit** — explicit processing confirmation via `ack.acknowledge()` instead of automatic.
- **`enable.auto.commit: false`** — disables automatic offset commit at the Kafka client level.
- **`ack-mode: MANUAL_IMMEDIATE`** — Spring Kafka does not accumulate acknowledgments, commits immediately.
- **`Acknowledgment` parameter in `@KafkaListener`** — Spring passes the acknowledgment object to the handler.
- **`ack.nack(Duration)`** — reject a message with a delayed retry.
- **At-least-once delivery** — guarantee that a message will be processed at least once.
- **At-most-once delivery** — risk with auto-commit: offset is committed before processing completes.
- **Failure simulation** — endpoint `POST /api/notifications/simulate-failure/{count}` for demo.
- **`redelivered` counter** — field in `/count` response for tracking re-deliveries.
- **Idempotency requirement** — with at-least-once, the handler must be idempotent.

## What changed compared to branch05

- `analytics-service` removed — focus is only on the commit mechanism in `notification-service`.
- `enable.auto.commit` changed to `false` — Kafka no longer commits offset automatically.
- Added `ack-mode: MANUAL_IMMEDIATE` in `spring.kafka.listener`.
- `@KafkaListener` now accepts a second parameter `ack: Acknowledgment`.
- Added `ack.acknowledge()` after successful processing.
- Added `ack.nack(Duration.ofSeconds(2))` for simulating failure with retry.
- New endpoint `POST /api/notifications/simulate-failure/{count}` — schedule N failures.
- `/count` response now includes `redelivered` field — number of re-delivered messages.
- Topic names changed to `06.orders.*`.

## Architecture

```
┌──────────────────────────────────────────────────┐
│  order-service :8081                             │
│  POST /api/orders  →  kafkaTemplate.send()       │
└─────────────────┬────────────────────────────────┘
                  │
     ┌────────────┴───────────┐
     │                        │
 06.orders.created        06.orders.cancelled
   partitions: 3             partitions: 1
     │                        │
     └────────────┬───────────┘
                  │ notification-service-group
                  ▼
   notification-service-b06 :8082
   ┌─────────────────────────────────────────┐
   │ handleOrderCreated(record, ack):        │
   │   if failNextN > 0:                     │
   │     ack.nack(Duration.ofSeconds(2))     │
   │   else:                                 │
   │     process() → ack.acknowledge()       │
   └─────────────────────────────────────────┘
```

```
__consumer_offsets:
  notification-service-group | 06.orders.created | partition 0 → offset N
  (offset N is committed only after ack.acknowledge(), not automatically)
```

### Topics and their configuration

- `06.orders.created` → partitions: 3, retention: 7 days,
  read by `notification-service-group` with manual commit.
- `06.orders.cancelled` → partitions: 1, retention: 7 days,
  read by `notification-service-group` with manual commit.

## Key concepts of this branch

### Auto-commit — risk of message loss

With `enable.auto.commit=true` (default) Kafka automatically commits offset every 5 seconds
(or on each `poll()`).
If the service crashes **between receiving** and **processing** — the offset is already committed:

```
Batch received:   [offset 10, offset 11, offset 12]
Auto-commit:      committed offset = 12  ← committed immediately
Processing:       offset 10 → OK
                  offset 11 → CRASH 💥
Restart:          committed offset = 12 → offset 11 is already SKIPPED (at-most-once)
```

### Manual commit — at-least-once

With `MANUAL_IMMEDIATE` offset is committed only after explicit `ack.acknowledge()` call:

```
Batch received:   [offset 10, offset 11]
Processing:       offset 10 → OK → ack.acknowledge() → committed = 10
                  offset 11 → CRASH 💥 (ack not called)
Restart:          committed = 10 → offset 11 is read again (at-least-once)
```

> Duplicate possible: if `ack.acknowledge()` was called but service crashed **after** processing —
> on restart the message arrives again. Therefore the handler must be **idempotent**.

### ack.acknowledge() and ack.nack(Duration)

```kotlin
@KafkaListener(topics = ["06.orders.created"], groupId = "notification-service-group")
fun handleOrderCreated(
    record: ConsumerRecord<String, OrderCreatedEvent>,
    ack: Acknowledgment                                   // ← Spring passes Acknowledgment
) {
    if (failNextN.get() > 0) {
        failNextN.decrementAndGet()
        redeliveredCount.incrementAndGet()
        log.warn("[SIMULATED FAILURE] nacking partition={} offset={} — redelivery in 2s",
            record.partition(), record.offset())
        ack.nack(Duration.ofSeconds(2))                  // ← offset NOT committed, retry in 2s
        return
    }
    // ... processing ...
    ack.acknowledge()                                     // ← offset committed immediately
}
```

- `ack.nack(Duration)` — offset remains uncommitted.
  Kafka delivers the same message again after the specified delay.
  Next messages in the partition **are not read** until the current one is acknowledged.
- `ack.acknowledge()` — offset is committed. Consumer moves forward.

### Manual commit configuration in application.yml

```yaml
# notification-service/application.yml
spring:
  kafka:
    consumer:
      enable-auto-commit: false       # ← Kafka client does not commit automatically
    listener:
      ack-mode: manual_immediate      # ← Spring Kafka expects explicit ack.acknowledge()
```

Without `ack-mode: manual_immediate`, Spring Kafka will call `acknowledge()` itself
after the listener method completes — even if you don't call it manually.

### Comparison: at-most-once vs at-least-once

```
Auto-commit (at-most-once):
  received → commit → process → crash → message LOST ❌

Manual commit (at-least-once):
  received → process → commit → crash? → read again ✅ (but duplicate possible)

Exactly-once (branch12):
  transactional producer + read_committed consumer → no losses, no duplicates ✅
```

## How to run

```bash
docker compose -f docker-compose-06.yml up --build
```

Check 4 containers:

```bash
docker compose -f docker-compose-06.yml ps
# kafka, kafka-ui, order-service-b06, notification-service-b06
```

## How to test

### 1. Basic check — manual commit works

```bash
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-01","product":"Book","quantity":1,"totalAmount":25.00}'

curl http://localhost:8082/api/notifications/count
# {"instanceId":"notification-service-1","orders_created":1,"orders_cancelled":0,"redelivered":0,"total":1}
```

**Windows (PowerShell):**

```powershell
Invoke-RestMethod -Method POST -Uri http://localhost:8081/api/orders `
  -ContentType "application/json" `
  -Body '{"userId":"user-01","product":"Book","quantity":1,"totalAmount":25.00}'

Invoke-RestMethod http://localhost:8082/api/notifications/count
```

### 2. Main demo scenario: simulate failure → at-least-once

```bash
# Step 1: Schedule 2 failures for next messages
curl -s -X POST http://localhost:8082/api/notifications/simulate-failure/2
# {"instanceId":"notification-service-1","scheduledNacks":2,"message":"Next 2 message(s) will be nacked and redelivered after 2s"}

# Step 2: Send an order
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-fail","product":"Doomed Order","quantity":1,"totalAmount":99.00}'

# notification-service logs:
# [SIMULATED FAILURE] nacking partition=X offset=Y — redelivery in 2s
# (after 2s) [SIMULATED FAILURE] nacking ...
# (after 2s) ╔══ ORDER CREATED #1 ... ╚══
```

```bash
# Step 3: Verify — orders_created=1, redelivered=2
curl http://localhost:8082/api/notifications/count
# {"orders_created":1,"orders_cancelled":0,"redelivered":2,"total":1}
```

### 3. Check offset via CLI

```bash
# During failure (nack active) — LAG > 0:
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group notification-service-group

# After successful processing — LAG = 0
```

### 4. Kafka UI

Open: `http://localhost:8080`

- `Consumer Groups` → `notification-service-group` → watch LAG change during nack.
- `Topics` → `06.orders.created` → see offset after successful `acknowledge()`.

## How it works internally

### Producer (unchanged from branch05)

Topic names changed from `05.orders.*` to `06.orders.*` — just numbering refactoring.

### Consumer — notification-service (key changes)

```kotlin
// OrderEventListener.kt — branch06
@KafkaListener(topics = ["06.orders.created"], groupId = "notification-service-group")
fun handleOrderCreated(
    record: ConsumerRecord<String, OrderCreatedEvent>,
    ack: Acknowledgment          // ← NEW: Acknowledgment parameter
) {
    if (failNextN.get() > 0) {
        failNextN.decrementAndGet()
        redeliveredCount.incrementAndGet()
        ack.nack(Duration.ofSeconds(2))   // ← NEW: reject, retry in 2s
        return
    }
    // ... logging ...
    ack.acknowledge()                      // ← NEW: explicit offset commit
}
```

```yaml
# notification-service/application.yml — branch06
spring:
  kafka:
    consumer:
      enable-auto-commit: false       # ← NEW
    listener:
      ack-mode: manual_immediate      # ← NEW
```

## Project structure (changes relative to branch05)

```
kafka-laboratory/
├── docker-compose-06.yml                    ← no analytics-service, topics 06.*
├── branch06_offsets_commits/
│   ├── notification-service/
│   │   └── src/main/resources/
│   │       └── application.yml              ← +ack-mode, +enable-auto-commit=false
│   │   └── src/main/kotlin/.../listener/
│   │       └── OrderEventListener.kt        ← +Acknowledgment param, +nack, +acknowledge
│   │   └── src/main/kotlin/.../controller/
│   │       └── NotificationController.kt    ← +simulate-failure endpoint, +redelivered in /count
│   └── order-service/                       ← topics 06.orders.*
└── README06.md
```

## What's next — branch07

- **acks=0, acks=1, acks=all** — three producer acknowledgment modes and their impact on reliability.
- **Idempotent producer** — `enable.idempotence=true` and protection against duplicates on retry.
- **linger.ms + batch.size** — message batching for higher throughput.
- **delivery.timeout.ms** — maximum time for one delivery attempt with retries.
- **BenchmarkService** — comparing performance of three configurations.

---
---

## Presentation slides (11 slides)

**Slide 1: Branch 06 — Offsets & Commits**
- Apache Kafka for Certification & Production
- Manual commit vs auto-commit

**Slide 2: Agenda**
1. Auto-commit problem — at-most-once risk
2. Manual commit — at-least-once guarantee
3. `enable.auto.commit: false` + `ack-mode: MANUAL_IMMEDIATE`
4. `ack.acknowledge()` — explicit offset commit
5. `ack.nack(Duration)` — rejection with retry
6. Idempotency requirement
7. Demo: simulate-failure → redelivery
8. At-most-once vs at-least-once vs exactly-once
9. CCDAK exam tips

**Slide 3: Auto-Commit Problem**
- `enable.auto.commit=true` (default) — Kafka commits offset every 5 seconds
- Offset committed **before** processing completes
- If service crashes during processing — message is lost
- This is at-most-once: each message processed **at most** once

**Slide 4: Manual Commit — At-Least-Once**
- `enable.auto.commit: false` + `ack-mode: MANUAL_IMMEDIATE`
- Offset committed only after `ack.acknowledge()`
- If service crashes before `acknowledge()` — message arrives again
- This is at-least-once: each message processed **at least** once (duplicate possible)

**Slide 5: Acknowledgment API**
- `ack.acknowledge()` — commits offset immediately (MANUAL_IMMEDIATE)
- `ack.nack(Duration)` — rejects message, retry after Duration
- During nack state: next messages in partition are not read
- Spring passes `Acknowledgment` as parameter in `@KafkaListener`

**Slide 6: Demo — Simulate Failure**
- `POST /api/notifications/simulate-failure/2` — schedule 2 failures
- First message → nack → after 2s → nack → after 2s → acknowledge
- `/count`: orders_created=1, redelivered=2
- LAG > 0 during nack, LAG = 0 after acknowledge

**Slide 7: Idempotency Requirement**
- With at-least-once, duplicates are not an exception but **expected**
- Handler must be idempotent: repeated execution does not change state
- Idempotency examples: INSERT OR IGNORE, UPDATE WHERE state=X, check orderId
- If business logic is not idempotent → exactly-once needed (branch12)

**Slide 8: MANUAL_IMMEDIATE vs MANUAL**
- `MANUAL_IMMEDIATE` — commit happens immediately when ack.acknowledge() is called
- `MANUAL` — commit happens on next poll() (batch mode)
- For most use-cases — `MANUAL_IMMEDIATE` is simpler and more reliable
- `BATCH` — one ack for the entire batch at once

**Slide 9: Delivery Strategies**
- **at-most-once**: auto-commit → simple implementation, possible loss
- **at-least-once**: manual commit → delivery guarantee, possible duplicate
- **exactly-once**: transactions (branch12) → most complex, maximum reliability
- Choice depends on business requirements (finance vs analytics)

**Slide 10: Key Takeaways**
- `enable.auto.commit=false` + `ack-mode=MANUAL_IMMEDIATE` — production standard
- `ack.acknowledge()` after successful processing — at-least-once guarantee
- `ack.nack(Duration)` — controlled retry without message loss
- Idempotency — mandatory requirement with at-least-once
- Duplicates — normal in at-least-once systems

**Slide 11: What's Next — Branch 07: Producer Configuration**
- acks=0 vs acks=1 vs acks=all — three acknowledgment modes
- Idempotent producer — protection against duplicates on retry
- linger.ms + batch.size — batching for throughput
- BenchmarkService — performance measurement

## Presentation script

**Slide 1:**
Welcome to the sixth branch.
Previously we studied how messages are read and distributed between consumer groups.
Today we'll cover one of the most important questions for production: what happens to the offset
if the service crashes during processing?

**Slide 2:**
We'll cover nine topics: from the auto-commit problem to delivery strategies.
The main idea: offset should be committed only after confirmed processing.

**Slide 3:**
Auto-commit is convenient default behavior.
Kafka commits offset every 5 seconds regardless of whether we processed the message or not.
If the service crashes between auto-commit and completion of processing — the message is lost forever.
This is called at-most-once: better not to receive than to receive twice.

**Slide 4:**
Manual commit reverses the logic: process first, then commit.
If the service crashes before ack.acknowledge() — offset is not committed.
On restart Kafka delivers the message again.
This ensures at-least-once: the message will definitely be processed, but possibly twice.

**Slide 5:**
Spring Kafka provides Acknowledgment via a listener method parameter.
ack.acknowledge() commits offset immediately — this is MANUAL_IMMEDIATE mode.
ack.nack(Duration) rejects the message and retry will happen after the specified delay.
While the message is waiting for retry — subsequent ones in the partition are not read.

**Slide 6:**
In the demo we schedule two failures via POST /simulate-failure/2.
The first order is received — nack — after 2 seconds nack again — after 2 seconds success.
In the counter: orders_created=1 but redelivered=2 — we see how many retries were made.
This is exactly at-least-once in action.

**Slide 7:**
The main consequence of at-least-once: the handler must be idempotent.
If the same order arrives twice — repeated processing must not break the system.
For example: check whether orderId already exists in the database before inserting.
If this is technically impossible — exactly-once is needed (branch12 with transactions).

**Slide 8:**
MANUAL_IMMEDIATE is the most common mode for production.
Commit happens immediately when acknowledge is called — without delay.
MANUAL mode accumulates and commits on the next poll — for complex batch scenarios.
For most microservices MANUAL_IMMEDIATE is simpler and safer.

**Slide 9:**
Three delivery strategies: at-most-once for non-critical logs and metrics.
At-least-once — standard for most business services with idempotent handlers.
Exactly-once — for financial transactions where duplicates are unacceptable.
Choose the strategy based on business requirements, not technical preferences.

**Slide 10:**
Remember: enable.auto.commit=false and ack-mode=MANUAL_IMMEDIATE — this is production configuration.
Never rely on auto-commit for critical business events.
Always verify your handler is idempotent with manual commit.

**Slide 11:**
Next branch — Producer Configuration.
We'll focus on the producer side: acks, idempotence, linger.ms, batch.size.
We'll see how BenchmarkService measures performance of three configurations.

## Test Questions (up to 10 questions)

**Question 1:**
Consumer reads a message and service crashes **after** automatic commit but **before** processing completes.
What delivery guarantee does this scenario demonstrate?

A) At-least-once
B) At-most-once
C) Exactly-once
D) No guarantee

**Answer:** B —
Auto-commit commits offset regardless of processing.
If service crashes after commit but before processing — message is lost forever (at-most-once).

---

**Question 2:**
Which combination of settings activates manual commit in Spring Kafka?

A) `ack-mode: auto` + `enable.auto.commit: true`
B) `ack-mode: manual_immediate` + `enable.auto.commit: false`
C) `ack-mode: batch` + `enable.auto.commit: false`
D) Only `enable.auto.commit: false`

**Answer:** B —
`enable.auto.commit: false` disables auto-commit at the Kafka client level.
`ack-mode: manual_immediate` tells Spring Kafka to commit only after explicit `ack.acknowledge()`.

---

**Question 3:**
`ack.nack(Duration.ofSeconds(2))` was called for message at offset=10, partition=1.
What happens to messages at offset=11, 12, 13 (same partition)?

A) They will be read immediately
B) They will wait until offset=10 is acknowledged
C) They will be read but without commit
D) Kafka will delete them

**Answer:** B —
`nack` does not commit offset.
Consumer will not proceed to next messages in the partition until the current one is acknowledged or redirected.

---

**Question 4:**
Consumer received a message, processed it successfully, called `ack.acknowledge()`, then crashed.
What happens on restart?

A) Message will be delivered again (at-least-once)
B) Message will not be delivered (offset already committed)
C) Kafka automatically determines whether retry is needed
D) Consumer starts reading from the beginning of the topic

**Answer:** B —
`ack.acknowledge()` already committed the offset.
On restart consumer starts from the next offset — message is not repeated.

---

**Question 5:**
Notification-service received an order twice due to at-least-once retry.
What technique avoids sending email twice to the same user?

A) Increase `max.poll.records`
B) Store orderId in the database and check before sending (idempotency check)
C) Use `acks=all`
D) Decrease `linger.ms`

**Answer:** B —
Idempotency check: before processing, check if this orderId was already processed.
If yes — skip without error. This is standard technique for at-least-once systems.

---

**Question 6:**
What is the difference between `MANUAL_IMMEDIATE` and `MANUAL` ack-mode in Spring Kafka?

A) `MANUAL_IMMEDIATE` — commit after each message; `MANUAL` — after batch
B) `MANUAL_IMMEDIATE` — commit on next poll; `MANUAL` — immediately
C) Both are the same, only names differ
D) `MANUAL` uses async commit; `MANUAL_IMMEDIATE` — sync

**Answer:** A —
`MANUAL_IMMEDIATE` commits offset immediately when `acknowledge()` is called.
`MANUAL` accumulates acknowledgments and commits on next `poll()`.

---

**Question 7:**
Which delivery guarantee is the standard for most microservices in production?

A) At-most-once — simplest implementation
B) At-least-once with idempotent handlers
C) Exactly-once always
D) No guarantee for maximum speed

**Answer:** B —
At-least-once with idempotent handlers — the gold standard.
Exactly-once is more complex and slower; at-most-once is risky for business data.

---

**Question 8:**
What is the `redelivered` value in the `/count` response if we scheduled 2 failures and sent 1 order?

A) 0
B) 1
C) 2
D) 3

**Answer:** C —
Each `ack.nack()` call increments `redeliveredCount`.
2 scheduled failures → 2 nack calls → `redelivered=2`, but `orders_created=1`.

---

**Question 9:**
Why is auto-commit dangerous for financial transactions?

A) Auto-commit is too slow for finance
B) Offset committed before processing completes → on crash transaction is lost
C) Auto-commit doesn't support partitions
D) Kafka doesn't allow auto-commit for topics with `retention.ms > 1 day`

**Answer:** B —
In financial transactions a lost message means an unregistered payment.
Auto-commit may commit offset before processing completes — and on crash the payment is lost.

---

**Question 10:**
How to configure a consumer so it NEVER commits offset automatically in Spring Kafka?

A) `spring.kafka.consumer.enable-auto-commit: false`
B) `spring.kafka.listener.ack-mode: manual_immediate`
C) Both together: A + B
D) `spring.kafka.consumer.auto-offset-reset: none`

**Answer:** C —
`enable-auto-commit: false` alone is not enough: Spring Kafka may still automatically call
`acknowledge()` if `ack-mode` is not set explicitly.
Both settings are required together.