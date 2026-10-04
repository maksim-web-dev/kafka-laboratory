# Branch 09 — Error Handling & Dead Letter Topic

## `branch09_error_retry` — what we learn

- **`@RetryableTopic`** — Spring Kafka annotation for non-blocking retry through separate Kafka topics.
- **Non-blocking retry** — failed messages are moved to retry topics; the main topic keeps being read.
- **Retry topics** — `09.orders.created-retry-0`, `09.orders.created-retry-1` (created automatically).
- **Dead Letter Topic (DLT)** — `09.orders.created-dlt` — final destination after N failed attempts.
- **`@DltHandler`** — Spring Kafka method for processing DLT messages (logging, alerting).
- **Exponential backoff** — `delay=1000ms, multiplier=5.0` → retry after 1s, then after 5s.
- **Retry headers** — Kafka headers with metadata: original topic, partition, exception message.
- **Blocking vs Non-blocking retry** — comparison: spring-retry (in-memory) vs retry topics (Kafka-persistent).
- **New service `payment-service`** — demonstrates retry for critical transactions.
- **`PaymentException`** — custom RuntimeException to simulate payment failure.

## What changed compared to branch08

- Instead of `notification-service`, the main consumer is now **`payment-service`**.
- Added `@RetryableTopic` with 3 attempts and exponential backoff (1s → 5s).
- 3 new topics created automatically: `retry-0`, `retry-1`, `dlt`.
- Added `@DltHandler` for processing messages after retries are exhausted.
- New endpoint `POST /api/payments/simulate-failure/{count}` for demo.
- New endpoint `GET /api/payments/stats` — statistics: processed/dlt/failNextN.
- Notification-service removed from this branch — focus is on error handling.
- Topic names changed to `09.orders.*`.

## Architecture

```
order-service-b09 :8081
  POST /api/orders → 09.orders.created
          │
          ▼
 payment-service-b09 :8083
 @RetryableTopic(attempts=3)
          │
  ┌───────┼───────────────────────────────────────────────────┐
  │       │  Attempt 1 (t=0)                                   │
  │  09.orders.created ──── success ──────────────────────► OK │
  │       │                                                     │
  │       │  failNextN > 0 → PaymentException                  │
  │       ▼                                                     │
  │  09.orders.created-retry-0 (delay 1s)                      │
  │       │  failNextN > 0 → PaymentException                  │
  │       ▼                                                     │
  │  09.orders.created-retry-1 (delay 5s)                      │
  │       │  failNextN > 0 → PaymentException                  │
  │       ▼                                                     │
  │  09.orders.created-dlt  → @DltHandler ← MANUAL REVIEW      │
  └───────────────────────────────────────────────────────────┘
```

### Topics and their configuration

- `09.orders.created` → partitions: 3, retention: 7 days, main topic.
- `09.orders.created-retry-0` → auto-created, 1s delay before processing.
- `09.orders.created-retry-1` → auto-created, 5s delay before processing.
- `09.orders.created-dlt` → auto-created, final destination after 3 failed attempts.

## Key Concepts of This Branch

### The Problem Without DLT

```
Consumer reads offset=42 → RuntimeException during processing
  Option A: retry indefinitely → consumer stuck at offset=42, entire topic blocked
  Option B: log & skip → message lost with no record
  Option C: catch all exceptions → processing "successful" regardless of outcome

None of these options is acceptable for production.
```

### Non-blocking Retry Through Retry Topics

```
Attempt 1 (t=0ms):    topic: 09.orders.created, offset=42
  → PaymentException ✗
  → message moved to retry-0 (with header backoff_next_elapse)

Attempt 2 (t=1000ms): topic: 09.orders.created-retry-0
  → PaymentException ✗
  → message moved to retry-1

Attempt 3 (t=6000ms): topic: 09.orders.created-retry-1
  → PaymentException ✗
  → message moved to dlt

@DltHandler:          topic: 09.orders.created-dlt
  → logging, alerting, manual review
```

**Non-blocking**: while `offset=42` waits in retry-0 for 1 second,
offsets 43, 44, 45 are read from `09.orders.created` and processed normally.

### @RetryableTopic Annotation

```kotlin
// OrderPaymentListener.kt
@RetryableTopic(
    attempts = "3",               // 1 main + 2 retry = 3 attempts total
    backoff = Backoff(
        delay = 1000,             // first retry after 1000ms
        multiplier = 5.0          // second retry after 1000 * 5 = 5000ms
    ),
    topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,  // -retry-0, -retry-1
    dltStrategy = DltStrategy.FAIL_ON_ERROR
)
@KafkaListener(topics = ["09.orders.created"], groupId = "payment-service-group")
fun handleOrderCreated(
    record: ConsumerRecord<String, OrderCreatedEvent>,
    @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String
) {
    if (failNextN.get() > 0) {
        failNextN.decrementAndGet()
        throw PaymentException("Simulated payment failure for order ${record.value().orderId}")
    }
    // ... payment processing ...
}

@DltHandler
fun handleDlt(
    record: ConsumerRecord<String, OrderCreatedEvent>,
    @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String
) {
    dltCount.incrementAndGet()
    log.error("[DLT] Order {} → Manual intervention required!", record.value().orderId)
}
```

### Retry Headers in Kafka

Every message in a retry/dlt topic carries headers:

```
kafka_original_topic:      09.orders.created
kafka_original_partition:  0
kafka_original_offset:     42
kafka_exception-message:   Simulated payment failure for order abc-123
kafka_backoff_next_elapse: 1725000000000  ← timestamp when to process
```

### Blocking vs Non-blocking Retry

```
Blocking (Spring Retry / DefaultErrorHandler):
  offset=42 fails → retry in-memory → entire consumer waits
  throughput: STOPS until retries exhausted
  retry state: in memory → lost on restart
  visibility: hidden (not visible in Kafka UI)

Non-blocking (@RetryableTopic):
  offset=42 fails → moved to retry-topic
  main topic: keeps being read (offset=43, 44, ...)
  retry state: in Kafka → survives restart
  visibility: visible in Kafka UI as separate topics
```

## How to Run

```bash
docker compose -f docker-compose-09.yml up --build
```

Verify 4 containers:

```bash
docker compose -f docker-compose-09.yml ps
# kafka, kafka-ui, order-service-b09, payment-service-b09
```

## How to Test

### 1. Successful processing (no failures)

```bash
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-01","product":"Book","quantity":1,"totalAmount":25.00}'

curl http://localhost:8083/api/payments/stats
# {"processed":1,"dlt":0,"failNextN":0}
```

### 2. Main demo scenario: retry → DLT

```bash
# Step 1: Schedule 3 failures (one message will go through all attempts and land in DLT)
curl -s -X POST http://localhost:8083/api/payments/simulate-failure/3
# {"scheduledFailures":3,"message":"Next 3 invocations will throw PaymentException → DLT"}

# Step 2: Send an order
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-fail","product":"Doomed Order","quantity":1,"totalAmount":99.00}'

# payment-service logs:
# [RETRY] Attempt on topic=09.orders.created, orderId=abc-123
# (after 1s)
# [RETRY] Attempt on topic=09.orders.created-retry-0, orderId=abc-123
# (after 5s)
# [RETRY] Attempt on topic=09.orders.created-retry-1, orderId=abc-123
# [DLT] ⚠ Order abc-123 → Manual intervention required!

# Step 3: Check statistics
curl http://localhost:8083/api/payments/stats
# {"processed":0,"dlt":1,"failNextN":0}
```

**Windows (PowerShell):**

```powershell
Invoke-RestMethod -Method POST -Uri http://localhost:8083/api/payments/simulate-failure/3
Invoke-RestMethod -Method POST -Uri http://localhost:8081/api/orders `
  -ContentType "application/json" `
  -Body '{"userId":"user-fail","product":"Doomed Order","quantity":1,"totalAmount":99.00}'
```

### 3. Partial retry — success after 1 attempt

```bash
# 1 failure → message will be processed on retry-0
curl -s -X POST http://localhost:8083/api/payments/simulate-failure/1

curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-lucky","product":"Lucky Order","quantity":1,"totalAmount":50.00}'

# Logs:
# [RETRY] Attempt on topic=09.orders.created → fail
# (after 1s)
# ╔═══ PAYMENT PROCESSED #1 (topic=09.orders.created-retry-0) ╗
```

### 4. Kafka UI — check retry topics

Open: `http://localhost:8080`

- `Topics` → see 4 topics:
  - `09.orders.created`
  - `09.orders.created-retry-0`
  - `09.orders.created-retry-1`
  - `09.orders.created-dlt`
- `09.orders.created-dlt` → Messages → see Headers with retry metadata.

### 5. Check retry headers via CLI

```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic 09.orders.created-dlt \
  --from-beginning \
  --property print.headers=true \
  --max-messages 1
```

## How It Works Internally

### Producer (unchanged from branch08)

`order-service` publishes to `09.orders.created` with key `userId`.

### Consumer — payment-service (NEW)

```kotlin
// OrderPaymentListener.kt — key parts

@RetryableTopic(attempts = "3", backoff = Backoff(delay = 1000, multiplier = 5.0),
    topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
    dltStrategy = DltStrategy.FAIL_ON_ERROR)
@KafkaListener(topics = ["09.orders.created"], groupId = "payment-service-group")
fun handleOrderCreated(
    record: ConsumerRecord<String, OrderCreatedEvent>,
    @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String
) {
    val event = record.value()
    if (failNextN.get() > 0) {
        failNextN.decrementAndGet()
        log.warn("[RETRY] Attempt on topic={}, orderId={}", topic, event.orderId)
        throw PaymentException("Simulated payment failure for order ${event.orderId}")
    }
    processedCount.incrementAndGet()
    log.info("╔══ PAYMENT PROCESSED #{}", processedCount.get())
    log.info("║  Topic: {}  Partition: {}  Offset: {}", topic, record.partition(), record.offset())
}

@DltHandler
fun handleDlt(record: ConsumerRecord<String, OrderCreatedEvent>,
              @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String) {
    dltCount.incrementAndGet()
    log.error("║  ⚠ DEAD LETTER TOPIC  ⚠")
    log.error("║  Order ID: {}  → Manual intervention required!", record.value().orderId)
}
```

```yaml
# payment-service/application.yml
server:
  port: 8083
spring:
  kafka:
    consumer:
      group-id: payment-service-group
      auto-offset-reset: earliest
```

## Project Structure (changes from branch08)

```
kafka-laboratory/
├── docker-compose-09.yml                              ← order + payment (no notification)
├── branch09_error_retry/
│   ├── order-service/                                 ← topic 09.orders.created
│   └── payment-service/                               ← NEW service
│       └── src/main/kotlin/.../listener/
│           └── OrderPaymentListener.kt                ← @RetryableTopic, @DltHandler
│       └── src/main/kotlin/.../controller/
│           └── PaymentController.kt                   ← /stats, /simulate-failure
│       └── src/main/kotlin/.../exception/
│           └── PaymentException.kt                    ← RuntimeException subclass
│       └── src/main/resources/
│           └── application.yml
└── README09.md
```

## What's Next — branch10

- **JSON Serialization** — `JsonSerializer` / `JsonDeserializer` with Spring Kafka.
- **`spring.json.type.mapping`** — alias instead of FQCN in the `__TypeId__` header.
- **Event versioning** — `eventVersion` field for backward compatibility.
- **Nested objects** — `List<OrderItem>` inside `OrderCreatedEvent`.
- **Tri-service pipeline** — order → payment → notification through two topics.

---
---

## Presentation Slides (11 slides)

**Slide 1: Branch 09 — Error Handling & Dead Letter Topic**
- Apache Kafka for Certification & Production
- @RetryableTopic, DLT, non-blocking retry

**Slide 2: Agenda**
1. The problem without error handling
2. Blocking vs Non-blocking retry
3. @RetryableTopic — configuration
4. Retry Topics — automatic creation
5. Exponential Backoff
6. @DltHandler
7. Retry Headers
8. Demo: retry → DLT
9. Key takeaways & CCDAK

**Slide 3: The Problem Without Error Handling**
- Option A: retry indefinitely → consumer stuck, topic blocked
- Option B: log & skip → message lost forever
- Option C: catch all exceptions → processing "successful" regardless of outcome
- None of these work for production-critical services

**Slide 4: Blocking vs Non-blocking Retry**
- **Blocking**: retry in-memory, entire consumer waits, state lost on restart
- **Non-blocking**: retry through Kafka topics, main topic keeps being read
- Non-blocking preserves throughput and persistence of retry state
- @RetryableTopic — Spring Kafka implementation of non-blocking retry

**Slide 5: @RetryableTopic — Configuration**
- `attempts="3"`: 1 main + 2 retry
- `delay=1000, multiplier=5.0`: backoff 1s → 5s
- `topicSuffixingStrategy`: suffix -retry-0, -retry-1
- `dltStrategy=FAIL_ON_ERROR`: DLT handler can throw exception

**Slide 6: Retry Topics — Auto-Created**
- Spring Kafka automatically creates retry and DLT topics
- `09.orders.created-retry-0`: 1s delay
- `09.orders.created-retry-1`: 5s delay
- `09.orders.created-dlt`: final destination
- All topics visible in Kafka UI

**Slide 7: @DltHandler**
- Called after N failed attempts (exhausted retries)
- Message will NOT be retried after the DLT handler
- Use cases: logging, Slack/PagerDuty alert, write to DB for manual review
- `dltStrategy=FAIL_ON_ERROR`: exception in @DltHandler is not silenced

**Slide 8: Retry Headers**
- Every message carries metadata about retries
- `kafka_original_topic`, `kafka_original_offset` — where it came from
- `kafka_exception-message` — reason for failure
- `kafka_backoff_next_elapse` — timestamp when to process
- Useful for audit and debugging in Kafka UI

**Slide 9: Demo — Retry → DLT**
- `simulate-failure/3` → 3 failures → one message goes through all 3 attempts → DLT
- Logs show the topic of each attempt: created → retry-0 → retry-1 → dlt
- stats: processed=0, dlt=1
- Partial retry: `simulate-failure/1` → success on retry-0

**Slide 10: Key Takeaways**
- Non-blocking retry through Kafka topics — production standard
- @RetryableTopic = auto-creation of retry/dlt + retry headers + @DltHandler
- Exponential backoff prevents overload during temporary failures
- DLT — mandatory for any critical consumer
- Retry state in Kafka — survives service restart

**Slide 11: What's Next — Branch 10: JSON Serialization**
- spring.json.type.mapping — alias instead of FQCN
- Event versioning — backward compatibility
- Nested objects: List<OrderItem>
- 3-service pipeline: order → payment → notification

## Presentation Script

**Slide 1:**
Branch nine — Error Handling and Dead Letter Topic.
This is one of the most important topics for production: what to do when a consumer cannot process a message?
A wrong approach can lead to data loss or the entire pipeline coming to a halt.

**Slide 2:**
We will cover nine topics from the problem to the solution.
Key insight: proper error handling is an architectural decision, not just a try-catch.

**Slide 3:**
Without error handling there are three bad options.
First: retry indefinitely — the consumer gets stuck on one message and stops reading further.
Second: log and skip — data is lost forever, nobody knows what happened.
Third: catch all — processing "successful" even when the payment was not processed.

**Slide 4:**
Blocking retry — spring-retry or DefaultErrorHandler — keeps the consumer blocked during retry.
Non-blocking — @RetryableTopic — moves the failed message to a separate topic.
While it waits there — the main topic keeps being read.
Retry state is stored in Kafka and survives service restart.

**Slide 5:**
@RetryableTopic is configured directly on the listener method.
attempts=3 means one main attempt plus two retry attempts.
delay=1000ms and multiplier=5.0 give exponential backoff: 1 second then 5 seconds.
dltStrategy=FAIL_ON_ERROR means an exception in @DltHandler will not be silenced.

**Slide 6:**
Spring Kafka automatically creates retry and DLT topics on first startup.
SUFFIX_WITH_INDEX_VALUE gives the suffixes -retry-0 and -retry-1.
All four topics are visible in Kafka UI — a big advantage over blocking retry.
You can see how many messages are stuck and at which stage.

**Slide 7:**
@DltHandler — the last line of defense.
After all retries are exhausted the message lands here.
Here you should: write to the database for manual review, send an alert to Slack or PagerDuty.
dltStrategy=FAIL_ON_ERROR guarantees that an exception in @DltHandler is not silently ignored.

**Slide 8:**
Retry headers — metadata added to every message in a retry topic.
They allow tracking where the message came from and why it failed.
kafka_backoff_next_elapse — the exact timestamp when the retry consumer should attempt processing.
This is the "schedule" for the retry scheduler.

**Slide 9:**
In the demo there are three scenarios.
First: three consecutive failures — the message goes through retry-0, retry-1, and lands in DLT.
Second: one failure — success on retry-0, meaning the message was processed not immediately but successfully.
Third: no failures — processed=1, dlt=0, everything is fine.

**Slide 10:**
Main takeaways: @RetryableTopic — a ready-made solution for production error handling.
Non-blocking retry preserves throughput during temporary failures.
DLT — mandatory for any critical consumer.
Retry state in Kafka survives restart unlike in-memory retry.

**Slide 11:**
The next branch — JSON Serialization.
We will explore how to safely transfer complex objects between services.
spring.json.type.mapping solves the ClassNotFoundException problem during cross-service deserialization.
And we will build a three-service pipeline: order, payment and notification.

## Test Questions (up to 10 questions)

**Question 1:**
`@RetryableTopic(attempts="3", backoff=@Backoff(delay=1000, multiplier=5.0))`.
After how much time will the third attempt occur (from the moment of the first failure)?

A) 1000ms
B) 5000ms
C) 6000ms (1000 + 5000)
D) 11000ms (1000 + 5000 + 5000)

**Answer:** C —
Attempt 1 (t=0): failure → delay 1000ms.
Attempt 2 (t=1000): failure → delay 1000 * 5 = 5000ms.
Attempt 3 (t=6000): third attempt.

---

**Question 2:**
What is the main advantage of non-blocking retry (through Kafka topics) over blocking retry (Spring Retry)?

A) Non-blocking retry is faster
B) While a message waits in the retry topic, the main topic keeps being read
C) Non-blocking retry does not require a Kafka connection
D) Non-blocking retry supports more attempts

**Answer:** B —
Non-blocking: the failed message is moved to a retry-topic.
The main topic keeps being read → throughput does not drop during retry waiting.

---

**Question 3:**
A consumer received a message, tried 3 times (all failed), the message landed in DLT.
`@DltHandler` threw an exception. What will happen with `dltStrategy=FAIL_ON_ERROR`?

A) Exception is silenced — message is considered processed
B) Message is sent to a new retry topic
C) Exception is logged and Spring Kafka applies the standard error handler
D) Consumer stops and requires manual restart

**Answer:** C —
`FAIL_ON_ERROR`: exception in @DltHandler is not silenced.
Spring Kafka logs it and applies the ErrorHandler (by default — log and continue).
The message is NOT sent for reprocessing.

---

**Question 4:**
What information does the Kafka header `kafka_backoff_next_elapse` contain?

A) Time elapsed from the first attempt to the current moment
B) Timestamp (milliseconds) when the consumer should process the message
C) Delay between attempts in milliseconds
D) Maximum number of attempts

**Answer:** B —
`kafka_backoff_next_elapse` — an absolute timestamp (epoch ms) when the retry consumer
should attempt to process the message.
The consumer reads the header and waits until that moment.

---

**Question 5:**
How many Kafka topics does `@RetryableTopic(attempts="3")` create automatically?

A) 1 (DLT only)
B) 2 (retry-0 + DLT)
C) 3 (retry-0 + retry-1 + DLT)
D) 4 (retry-0 + retry-1 + retry-2 + DLT)

**Answer:** B —
attempts=3 → 1 main + 2 retry attempts.
Retry topics = attempts - 1 = 2. But one of them is the DLT.
That is: retry-0 (attempt 2) + DLT (attempt 3). A total of 2 additional topics.

Clarification: `attempts=3` → 1 attempt in main topic + 1 in retry-0 + 1 in retry-1 (DLT).
Topics: retry-0, retry-1/DLT or retry-0 + DLT = 2 new topics (+ main = 3 total).

---

**Question 6:**
A consumer is processing `09.orders.created-retry-0`. The attempt failed.
Where will the message go next?

A) Back to `09.orders.created`
B) To `09.orders.created-retry-1`
C) To `09.orders.created-dlt` (immediately after retry-0)
D) Consumer will keep retrying on `retry-0` indefinitely

**Answer:** B —
Retry topics are sequential: main → retry-0 → retry-1 → DLT.
Failure on retry-0 → message is moved to retry-1.

---

**Question 7:**
What is the difference between `@RetryableTopic` and `DefaultErrorHandler` with `BackOff`?

A) DefaultErrorHandler is non-blocking; @RetryableTopic is blocking
B) @RetryableTopic is non-blocking (through Kafka topics); DefaultErrorHandler is blocking (in-memory)
C) They are the same — just different APIs
D) DefaultErrorHandler supports DLT; @RetryableTopic does not

**Answer:** B —
DefaultErrorHandler: retry in memory, consumer blocks between attempts, state lost on restart.
@RetryableTopic: retry through separate Kafka topics, non-blocking, state in Kafka.

---

**Question 8:**
What is the value of `stats.dlt` after: simulate-failure/1, send order, send order, send order?

A) 0 — only 1 failure, message will be processed on retry-0
B) 1 — the first order will go through DLT
C) 3 — every order will have 1 failure
D) Impossible to determine

**Answer:** A —
simulate-failure/1 schedules only 1 failure.
First order: attempt 1 → fail, attempt 2 (retry-0) → success.
Next two orders: no failures → success on main topic.
dlt=0.

---

**Question 9:**
A consumer received a message in DLT. `@DltHandler` wrote it to the database and returned successfully.
Can an admin later "replay" this message?

A) No — DLT messages are deleted after processing
B) Yes — DLT is a regular Kafka topic, can be read through a consumer or CLI
C) Only if retention has not expired and only through Admin API
D) No — @DltHandler automatically deletes the message

**Answer:** B —
DLT — a regular Kafka topic with retention.
Admin can: read via `kafka-console-consumer --from-beginning`,
send to a new topic for reprocessing, or start a separate consumer.

---

**Question 10:**
`@RetryableTopic(attempts="1")`. What will happen on failure?

A) One retry attempt, then DLT
B) Message will go directly to DLT (without retry)
C) Configuration error — minimum 2 attempts required
D) Consumer will retry indefinitely

**Answer:** B —
attempts=1 means 1 attempt total (the main one).
On failure → directly to DLT (0 retry topics).
Spring Kafka allows attempts=1.