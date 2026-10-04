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

# Branch 09
# Error Handling & Dead Letter Topic

**Apache Kafka for Certification & Production**

---

## Agenda

1. The problem without error handling
2. Blocking vs Non-blocking retry
3. `@RetryableTopic` — configuration
4. Retry topics — automatic creation
5. Exponential Backoff
6. `@DltHandler` — the last line of defense
7. Retry headers — metadata for debugging
8. Demo: retry → DLT flow
9. Key Takeaways & What's Next

---

## The Problem Without Error Handling

**Three bad approaches — none suitable for production:**

```
Option A: retry indefinitely (blocking)
  consumer retries in a loop
  → partition blocked, other messages not processed
  → if service restarts: retry state LOST

Option B: log & skip
  catch exception, log, commit offset
  → message LOST forever
  → no audit trail, no recovery

Option C: catch all exceptions
  catch(Exception e) → acknowledge()
  → processing "succeeded" regardless of outcome
  → payment failed silently, user not notified
```

> For critical business services: none of these are acceptable.

---

## Blocking vs Non-blocking Retry

**Key architectural difference:**

```
Blocking retry (DefaultErrorHandler):
  poll() → message fails → retry in-memory N times
  → consumer is blocked for all retry attempts
  → other messages in partition wait
  → retry state LOST on service restart

Non-blocking retry (@RetryableTopic):
  poll() → message fails → publish to retry-topic-0
  → main topic continues reading immediately
  → retry-topic consumer picks up after delay
  → retry state PERSISTED in Kafka (survives restart)
```

<br>

> Non-blocking retry = main topic never blocked by failing messages.

---

## @RetryableTopic — Configuration

**Declare retry behavior directly on the listener:**

```kotlin
@RetryableTopic(
    attempts = "3",                           // 1 main + 2 retries
    backoff = @Backoff(
        delay = 1000,                         // first retry: 1 second
        multiplier = 5.0                      // second retry: 5 seconds
    ),
    topicSuffixingStrategy = SUFFIX_WITH_INDEX_VALUE,
    dltStrategy = FAIL_ON_ERROR
)
@KafkaListener(topics = ["09.orders.created"])
fun handleOrderCreated(event: OrderCreatedEvent, ack: Acknowledgment) {
    notificationService.processOrder(event)
    ack.acknowledge()
}

@DltHandler
fun handleDlt(event: OrderCreatedEvent) {
    log.error("DLT: exhausted retries for order {}", event.orderId)
    alertService.notifyOncall(event)
}
```

---

## Retry Topics — Automatically Created

**Spring Kafka creates retry and DLT topics at startup:**

```
09.orders.created           ← main topic
09.orders.created-retry-0   ← first retry (delay: 1 second)
09.orders.created-retry-1   ← second retry (delay: 5 seconds)
09.orders.created-dlt       ← dead letter topic (final destination)
```

<br>

**All visible in Kafka UI:**
- See how many messages are in each retry stage
- Monitor DLT accumulation (growing DLT = recurring errors)
- Check retry headers for root cause

> Non-blocking retry makes the error pipeline observable and auditable.

---

## Exponential Backoff

**Prevent overload during transient failures:**

```
Attempt 1: main topic          → fails (DB timeout)
Wait 1 second
Attempt 2: retry-0 topic       → fails (DB still recovering)
Wait 5 seconds
Attempt 3: retry-1 topic       → succeeds ✓ (DB recovered)

Without backoff:
  retry every 100ms × 100 messages = storm of DB requests
  → worsens the outage instead of allowing recovery

With exponential backoff:
  gives the downstream system time to recover
  gradually reduces retry frequency
```

> Exponential backoff is essential when failures are caused by downstream saturation.

---

## @DltHandler — The Last Line of Defense

**Called after ALL retry attempts are exhausted:**

```kotlin
@DltHandler
fun handleDlt(
    event: OrderCreatedEvent,
    @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String,
    @Header(KafkaHeaders.EXCEPTION_MESSAGE) exceptionMessage: String
) {
    log.error("DLT message from {}: {}", topic, exceptionMessage)
    db.saveToDltAudit(event, exceptionMessage)     // persist for review
    slackService.alert("DLT received for order ${event.orderId}")
}
```

<br>

- Message will **NOT** be retried after `@DltHandler`
- `dltStrategy=FAIL_ON_ERROR`: exception in `@DltHandler` is NOT silenced
- **Required for every critical consumer** in production

---

## Retry Headers

**Each retry message carries metadata about its journey:**

```
kafka_original_topic:       09.orders.created
kafka_original_partition:   1
kafka_original_offset:      42
kafka_exception-classname:  java.sql.SQLTimeoutException
kafka_exception-message:    Connection timed out after 5000ms
kafka_backoff_next_elapse:  1696350012345  (timestamp: when to process)
```

<br>

- Visible in Kafka UI → useful for debugging without log diving
- `kafka_backoff_next_elapse` — the retry scheduler uses this to decide when to process
- `kafka_original_offset` — trace back to the exact original message

> Retry headers make the retry pipeline transparent and debuggable.

---

## Demo — Retry → DLT Flow

**Simulate 3 consecutive failures:**

```bash
# Plan 3 failures (exhausts all retries → goes to DLT)
curl -X POST http://localhost:8082/api/notifications/simulate-failure/3

# Send one order
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-01","product":"Book","quantity":1,"totalAmount":20.00}'
```

**Expected log sequence:**
```
[MAIN]    Processing order-001 → FAILED (attempt 1/3)
[RETRY-0] After 1s: order-001 → FAILED (attempt 2/3)
[RETRY-1] After 5s: order-001 → FAILED (attempt 3/3)
[DLT]     Dead letter: order-001 — all retries exhausted
```

**Stats:** `{"processed": 0, "dlt": 1}`

---

## Key Takeaways

- **Non-blocking retry** via Kafka topics = main topic never blocked by failures
- **`@RetryableTopic`** = auto-creates retry/DLT topics + retry headers + `@DltHandler`
- **Exponential backoff** prevents retry storms during downstream outages
- **DLT is mandatory** for every critical consumer in production
- **Retry state persists in Kafka** — survives service restarts
- **`dltStrategy=FAIL_ON_ERROR`** — exceptions in `@DltHandler` are not silenced
- **Retry headers** make the pipeline observable without log diving

<br>

> Correct error handling is an architectural decision, not just a try-catch block.

---

## What's Next — Branch 10

**JSON Serialization — cross-service type safety**

- `spring.json.type.mapping` — alias instead of FQCN in `__TypeId__`
- `spring.json.trusted.packages` — security whitelist
- Event versioning with `eventVersion` field
- Nested objects: `List<OrderItem>` serialization
- Full 3-service pipeline: order → payment → notification

---

<!-- _class: title -->

## What's Next — Branch 10
- Type mapping: alias decouples services
- trusted.packages: security best practice
- Event versioning: backward-compatible evolution
- 3-service pipeline with DLT as business event

# Questions?

**Branch 09 — Error Handling & Dead Letter Topic**