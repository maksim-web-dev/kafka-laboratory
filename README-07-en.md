# Branch 07 — Producer Configuration

## `branch07_producer_configuration` — what we learn

- **acks=0, acks=1, acks=all** — three write acknowledgment levels and their impact on reliability and latency.
- **ISR (In-Sync Replicas)** — the set of brokers synchronized with the partition leader.
- **Idempotent producer** — `enable.idempotence=true` with `producer-id` and `sequence number`.
- **Protection against duplicates on retry** — broker rejects repeated sends with the same sequence.
- **`linger.ms`** — waiting time to accumulate a larger batch before sending.
- **`batch.size`** — maximum batch size in bytes (16 KB / 32 KB / 64 KB).
- **`delivery.timeout.ms`** — total time limit for one send attempt with retries.
- **`retries`** — number of retry attempts on temporary network errors.
- **BenchmarkService** — comparing throughput and latency of three configurations.
- **`max.in.flight.requests.per.connection`** — number of parallel in-flight requests.

## What changed compared to branch06

- `notification-service` remains — back to auto-commit (focus is on producer config).
- `order-service` gets a production-safe default producer: `acks=all`, `enable.idempotence=true`.
- Added `BenchmarkService` and `BenchmarkController` — new endpoints for measurement.
- New topic `07.benchmark` — for benchmark messages (not business data).
- Three producer configurations: `acks0`, `acks1`, `acks-all` for comparison.
- `linger.ms=20` and `batch.size=65536` — batching for the `acks-all` configuration.
- Topic names changed to `07.orders.*`.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  order-service :8081                                        │
│                                                             │
│  Default producer (production-safe):                        │
│    acks=all, enable.idempotence=true, linger.ms=20          │
│                                                             │
│  BenchmarkService:                                          │
│    acks0Producer  → acks=0, linger=0, batch=16KB            │
│    acks1Producer  → acks=1, linger=5ms, batch=32KB          │
│    acksAllProducer → acks=all, idempotent, linger=20ms      │
└───────┬────────────────┬────────────────────────────────────┘
        │                │
  07.orders.created   07.benchmark
   (3 partitions)     (1 partition)
        │
        ▼
  notification-service-b07 :8082
  (auto-commit, only to verify messages are arriving)
```

### Topics and their configuration

- `07.orders.created` → partitions: 3, retention: 7 days, producer: default (acks=all, idempotent).
- `07.orders.cancelled` → partitions: 1, retention: 7 days.
- `07.benchmark` → partitions: 1, retention: 1 hour, used only for benchmarks.

## Key concepts of this branch

### acks — three acknowledgment modes

```
Producer → Broker Leader (partition N)
              │
  acks=0:     │  Wait for nothing — "fire and forget"
  acks=1:     │  Wait for "OK" from leader
  acks=all:   │  Wait for "OK" from leader + all ISR

ISR = In-Sync Replicas: brokers that keep replication up to date
```

- `acks=0` — Producer does not wait for any acknowledgment.
  Minimum latency, maximum throughput.
  Message may be lost if broker is unavailable.
  Suitable for: metrics, logs where individual loss is acceptable.
- `acks=1` — Producer waits for acknowledgment from the leader broker.
  Balance between reliability and speed.
  Message may be lost if leader crashes **before replication** to follower.
  Suitable for: internal events where duplicates are acceptable.
- `acks=all` — Producer waits for acknowledgment from leader **and all ISR**.
  Maximum reliability, higher latency.
  With `min.insync.replicas=2` and 2+ ISR: loss is impossible without broker data loss.
  Suitable for: critical business events (orders, payments).

### Idempotent Producer — protection against duplicates

Without idempotence: if producer sent a message but timed out before receiving ACK —
producer retries → broker writes a **duplicate**.

```
Without idempotence (acks=1, retries=3):
  Producer: send(orderId=ABC) → sequence not tracked
  Timeout (message arrived, but ACK lost)
  Retry: send(orderId=ABC) → broker writes AGAIN ← duplicate ❌

With enable.idempotence=true:
  Producer receives unique producer-id from broker
  Each message: producer-id + sequence-number (per partition)
  Retry: send(orderId=ABC, seq=5) → broker: "already have seq=5 from this producer" → skip ✓
```

**Requirements for idempotence:**

```yaml
producer:
  acks: all                                   # acks=all required
  retries: 2147483647                         # > 0
  properties:
    enable.idempotence: true
    max.in.flight.requests.per.connection: 5  # ≤ 5
```

### linger.ms + batch.size — batching

```
linger.ms=0 (default):
  msg-1 arrives → send batch[msg-1] immediately  ← 1 network request per message

linger.ms=20:
  t=0ms:  msg-1 → wait 20ms
  t=8ms:  msg-2 → add to batch
  t=15ms: msg-3 → add to batch
  t=20ms: → send batch[msg-1, msg-2, msg-3] ← 1 request instead of 3, better compression
```

- `batch.size` — maximum batch size in bytes.
  Batch is sent either when full, or when `linger.ms` has elapsed.
  Increasing batch.size increases throughput but increases first message latency.

### Three configurations for benchmark

```yaml
# acks=0: fire-and-forget — maximum speed
acks: "0"
linger.ms: 0
batch.size: 16384   # 16 KB
retries: 0

# acks=1: balance
acks: "1"
linger.ms: 5
batch.size: 32768   # 32 KB
retries: 3

# acks=all + idempotent: maximum reliability
acks: "all"
linger.ms: 20
batch.size: 65536   # 64 KB
enable.idempotence: true
max.in.flight.requests.per.connection: 5
delivery.timeout.ms: 30000
```

### delivery.timeout.ms

```
delivery.timeout.ms=30000 (30s):
  Total time from first send() to last retry or error.
  If all retries don't fit in 30s → DeliveryTimeoutException

retry.backoff.ms=100:
  Pause between retry attempts

Relationship:
  delivery.timeout.ms ≥ linger.ms + request.timeout.ms
```

## How to run

```bash
docker compose -f docker-compose-07.yml up --build
```

Check 4 containers:

```bash
docker compose -f docker-compose-07.yml ps
# kafka, kafka-ui, order-service-b07, notification-service-b07
```

## How to test

### 1. Benchmark a single mode

```bash
# acks=0: fire-and-forget, 200 messages
curl -s -X POST "http://localhost:8081/api/benchmark/run?mode=acks0&count=200" | python3 -m json.tool

# acks=1
curl -s -X POST "http://localhost:8081/api/benchmark/run?mode=acks1&count=200" | python3 -m json.tool

# acks=all + idempotent
curl -s -X POST "http://localhost:8081/api/benchmark/run?mode=acks-all&count=200" | python3 -m json.tool
```

Expected response:

```json
{
  "mode": "acks0",
  "acksConfig": "0",
  "idempotent": false,
  "lingerMs": 0,
  "batchSizeBytes": 16384,
  "messageCount": 200,
  "totalDurationMs": 45,
  "throughputMsgPerSec": 4444.4,
  "avgLatencyMs": 0.22
}
```

### 2. Compare all three modes (Linux)

```bash
curl -s -X POST "http://localhost:8081/api/benchmark/compare?count=200"
```

**Windows (PowerShell):**

```powershell
Invoke-RestMethod -Method POST -Uri "http://localhost:8081/api/benchmark/compare?count=200"
```

Expected result (with one broker):

```
mode      | throughput (msg/s) | avgLatency (ms)
acks0     | ~5000              | ~0.2
acks1     | ~1000              | ~1.0
acks-all  | ~800               | ~1.2
```

> Note: with one broker the difference between acks=1 and acks=all is minimal —
> only one ISR, both wait for one acknowledgment.
> In production with 3 brokers and min.insync.replicas=2 the difference is more significant.

### 3. Regular order via production-safe producer

```bash
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-01","product":"Book","quantity":1,"totalAmount":25.00}'
```

```bash
# order-service logs will show:
# OrderCreated published → topic=07.orders.created, partition=X, offset=Y, key=user-01
```

### 4. Kafka UI — producer metrics

Open: `http://localhost:8080`

- `Topics` → `07.benchmark` → see messages from benchmark with different configurations.
- `Brokers` → `Metrics` → look for `record-send-rate`, `record-error-rate`, `batch-size-avg`.

## How it works internally

### Producer — order-service (key changes)

Production-safe default configuration:

```yaml
# order-service/application.yml — branch07
spring:
  kafka:
    producer:
      acks: all                              # ← NEW: maximum reliability
      properties:
        enable.idempotence: true             # ← NEW: protection against duplicates
        max.in.flight.requests.per.connection: 5
        delivery.timeout.ms: 30000
        linger.ms: 20                        # ← NEW: batching
        batch.size: 65536                    # ← NEW: 64 KB batch
        retries: 2147483647
```

```kotlin
// BenchmarkService.kt (new)
fun runBenchmark(mode: String, count: Int): BenchmarkResult {
    val producer = when(mode) {
        "acks0"    -> acks0Producer
        "acks1"    -> acks1Producer
        "acks-all" -> acksAllProducer
        else -> throw IllegalArgumentException("Unknown mode: $mode")
    }
    val start = System.currentTimeMillis()
    repeat(count) { i ->
        producer.send("07.benchmark", "bench-key-$i", "msg-$i").get()
    }
    val duration = System.currentTimeMillis() - start
    return BenchmarkResult(mode, count, duration, ...)
}
```

### Consumer — notification-service

Reverted to auto-commit (focus is on producer side):
listens to `07.orders.created`, logs received messages — no logic changes.

## Project structure (changes relative to branch06)

```
kafka-laboratory/
├── docker-compose-07.yml                              ← no analytics, topics 07.*
├── branch07_producer_configuration/
│   ├── order-service/
│   │   └── src/main/resources/
│   │       └── application.yml                        ← acks=all, idempotence, linger, batch
│   │   └── src/main/kotlin/.../service/
│   │       ├── OrderService.kt                        ← standard publishing (unchanged)
│   │       └── BenchmarkService.kt                    ← NEW: runBenchmark(), compareAll()
│   │   └── src/main/kotlin/.../config/
│   │       └── KafkaTopicConfig.kt                    ← +07.benchmark topic
│   │   └── src/main/kotlin/.../model/
│   │       └── BenchmarkResult.kt                     ← NEW: data class
│   │   └── src/main/kotlin/.../controller/
│   │       └── BenchmarkController.kt                 ← NEW: /api/benchmark/run, /compare
│   └── notification-service/                          ← auto-commit (unchanged)
└── README07.md
```

## What's next — branch08

- **CooperativeStickyAssignor** — incremental rebalance instead of stop-the-world EAGER.
- **Static Group Membership** — `group.instance.id` to avoid unnecessary rebalance on restart.
- **`max.poll.records`** and **`max.poll.interval.ms`** — batch size tuning.
- **`session.timeout.ms`** and **`heartbeat.interval.ms`** — failure detection configuration.
- **Graceful shutdown** — `server.shutdown=graceful` + `@PreDestroy`.

---
---

## Presentation slides (11 slides)

**Slide 1: Branch 07 — Producer Configuration**
- Apache Kafka for Certification & Production
- acks, idempotence, batching

**Slide 2: Agenda**
1. acks=0, acks=1, acks=all — three reliability levels
2. ISR — In-Sync Replicas
3. min.insync.replicas and NotEnoughReplicasException
4. Idempotent Producer — sequence numbers
5. linger.ms + batch.size — batching
6. delivery.timeout.ms + retries
7. Three configurations for different use-cases
8. BenchmarkService — measurement
9. Key takeaways & CCDAK

**Slide 3: acks — Three Acknowledgment Levels**
- `acks=0`: fire-and-forget, possible loss, maximum speed
- `acks=1`: leader acknowledges, possible loss on failover, medium speed
- `acks=all`: all ISR acknowledge, minimum loss, lower speed

**Slide 4: ISR and min.insync.replicas**
- ISR: brokers synchronized with leader within replica.lag.time.max.ms
- `min.insync.replicas=2`: producer gets error if < 2 ISR available
- Typical production config: 3 brokers, RF=3, min.insync.replicas=2
- "Quorum write" — resilience to single broker failure while preserving data

**Slide 5: Idempotent Producer**
- Without idempotence: timeout + retry → duplicate in Kafka
- `enable.idempotence=true`: producer-id + sequence number per partition
- Broker rejects duplicates automatically (same producer-id + sequence)
- Requirements: acks=all, retries>0, max.in.flight≤5

**Slide 6: linger.ms + batch.size**
- `linger.ms=0`: send immediately — low latency, small batch
- `linger.ms=20`: wait 20ms — higher throughput, larger batch
- `batch.size=65536`: maximum 64KB in one batch
- Batch is sent: either when full, or when `linger.ms` elapsed

**Slide 7: delivery.timeout.ms**
- Total time from send() to success or error
- Includes all retries and backoff pauses
- If exhausted → DeliveryTimeoutException
- Rule: delivery.timeout ≥ linger.ms + request.timeout.ms

**Slide 8: Three Configurations**
- `acks=0, linger=0, batch=16KB` — metrics, logs (loss acceptable)
- `acks=1, linger=5ms, batch=32KB` — internal events (duplicates acceptable)
- `acks=all, linger=20ms, batch=64KB, idempotent` — business events (finance, orders)

**Slide 9: BenchmarkService Results**
- acks0: ~5000 msg/s, latency ~0.2ms (with one broker)
- acks1: ~1000 msg/s, latency ~1.0ms
- acks-all: ~800 msg/s, latency ~1.2ms (but with batching linger=20ms)
- With 3 brokers and min.insync.replicas=2 the acks1 vs acks-all difference is more significant

**Slide 10: Key Takeaways**
- `acks=all` + `enable.idempotence=true` — production standard for business events
- `linger.ms + batch.size` — trade-off between latency and throughput
- Idempotence protects against duplicates on retry at broker level
- `delivery.timeout.ms` — total time limit for one delivery attempt
- With one broker, acks=1 vs acks=all — minimal difference

**Slide 11: What's Next — Branch 08: Consumer Configuration**
- CooperativeStickyAssignor — incremental rebalance
- Static Group Membership — less rebalance on restart
- max.poll.records, session.timeout.ms — tuning
- Graceful shutdown

## Presentation script

**Slide 1:**
Seventh branch — Producer Configuration.
Previously we covered the consumer side: commit, lag, groups.
Now the focus is on the producer: how Kafka guarantees that a message reached the broker.

**Slide 2:**
We study three acknowledgment levels, idempotency, batching and measurement.
The main idea: producer configuration determines the balance between speed and reliability.

**Slide 3:**
acks=0: producer sends and waits for no response.
Maximum speed but if broker is unavailable — message is lost forever.
acks=1: wait for acknowledgment from the leader broker.
Message may be lost if leader crashed before replication to follower.
acks=all: wait for acknowledgment from all ISR — most reliable mode.

**Slide 4:**
ISR — In-Sync Replicas: brokers that stay synchronized with the leader.
min.insync.replicas defines how many ISR minimum must confirm.
With three brokers and min.insync.replicas=2: we can afford one broker failure.
If fewer than the minimum ISR remain — producer gets NotEnoughReplicasException.

**Slide 5:**
Idempotent producer solves the duplicate problem on retry.
Broker issues a producer-id at registration.
Each message has a sequence number unique to producer+partition.
On retry: if broker already has this sequence — it silently ignores the duplicate.

**Slide 6:**
linger.ms and batch.size — batching mechanism.
With linger=0 each message goes as a separate network request.
With linger=20ms producer waits and accumulates several messages in one batch.
Fewer network requests, better compression, higher throughput — at the cost of slightly higher latency.

**Slide 7:**
delivery.timeout.ms — total time budget for delivering one message.
If delivery doesn't succeed within this time with all retries — producer throws an exception.
Usually 30-120 seconds for production.
Important: delivery.timeout must be greater than linger.ms plus request.timeout.ms.

**Slide 8:**
Three configurations for three types of use-cases.
acks=0 for metrics and logs where individual loss is not critical.
acks=1 for internal events where speed matters more than perfect reliability.
acks=all with idempotency — for any business events involving money or data.

**Slide 9:**
BenchmarkService allows measuring the difference.
With one broker the difference between acks=1 and acks=all is small — only one ISR.
In production with three brokers acks=all is noticeably slower.
But for critical events this is an acceptable price for reliability.

**Slide 10:**
Main conclusions: acks=all and idempotence — production standard for business data.
linger.ms and batch.size — tools for throughput optimization.
delivery.timeout.ms limits the total time for delivery attempts.
Choose configuration based on business reliability requirements.

**Slide 11:**
Next branch — Consumer Configuration.
We'll study CooperativeStickyAssignor for soft rebalance.
Static Group Membership to reduce rebalance frequency on service restarts.
And graceful shutdown — how to properly terminate a consumer.

## Test Questions (up to 10 questions)

**Question 1:**
Producer sent a message with `acks=1`. Leader broker confirmed but crashed
**before** replication to follower. What loss will occur?

A) Message will not be lost — acks=1 guarantees reliability
B) Message will be lost — follower has no copy
C) Message will be restored from producer buffer
D) Kafka will automatically retry sending

**Answer:** B —
`acks=1` only guarantees that the leader received the message.
If leader crashes before replication — follower becomes new leader **without this message**.

---

**Question 2:**
What is the minimum value of `min.insync.replicas` for a topic with 3 replicas
that ensures resilience to one broker failure?

A) 1
B) 2
C) 3
D) Depends on `acks`

**Answer:** B —
With `min.insync.replicas=2` and 3 replicas: if 1 broker fails — 2 ISR remain ≥ min.
If 2 brokers fail — 1 ISR remains < min → `NotEnoughReplicasException`.

---

**Question 3:**
Producer sent a message (seq=42), got a timeout (broker wrote it but ACK was lost),
retried (seq=42). Broker received a duplicate. What happens with `enable.idempotence=true`?

A) Broker writes both — duplicate appears in topic
B) Broker rejects the second write — seq=42 already exists for this producer
C) Producer throws DuplicateKeyException
D) Consumer receives both with same offset

**Answer:** B —
With idempotence broker tracks `(producer-id, partition, sequence-number)`.
Duplicate with the same sequence number is silently ignored.

---

**Question 4:**
`linger.ms=20` and `batch.size=65536`. Producer received 3 messages totaling 70KB.
How many network requests will be sent?

A) 1 — batch combines all three
B) 2 — first batch fills to 64KB, second — remainder
C) 3 — each message separately (batch.size exceeded)
D) Depends on linger.ms

**Answer:** B —
Batch is sent when it reaches `batch.size` (64KB) or `linger.ms` elapses.
70KB doesn't fit in one batch → first batch ~64KB, second ~6KB.

---

**Question 5:**
Which setting is mandatory for `enable.idempotence=true`?

A) `acks=1`
B) `acks=all`
C) `linger.ms > 0`
D) `batch.size > 16384`

**Answer:** B —
Idempotent producer requires `acks=all`, `retries > 0`,
and `max.in.flight.requests.per.connection ≤ 5`.
Without `acks=all` Kafka throws `ConfigException`.

---

**Question 6:**
Producer set `delivery.timeout.ms=30000`. Sending attempt took 35 seconds due to retries.
What is returned in `whenComplete`?

A) Successful result — timeout is counted from each retry separately
B) `TimeoutException` — total time exceeded
C) `DeliveryTimeoutException` — Kafka-specific type
D) Message will be sent but without acknowledgment

**Answer:** C —
`delivery.timeout.ms` — total time from first `send()` to success or failure.
If all retries fit in 30s → OK. If not → `DeliveryTimeoutException`.

---

**Question 7:**
Which configuration is appropriate for sending monitoring metrics (1-2% loss acceptable)?

A) `acks=all, enable.idempotence=true`
B) `acks=0, linger.ms=0, retries=0`
C) `acks=1, linger.ms=5`
D) `acks=all, linger.ms=0`

**Answer:** B —
For non-critical metrics: `acks=0` — highest speed.
`retries=0` — no point retrying fire-and-forget.
`linger.ms=0` — minimum latency.

---

**Question 8:**
Why is `max.in.flight.requests.per.connection ≤ 5` a requirement for idempotent producer?

A) More than 5 parallel requests overload the broker
B) With > 5 parallel requests, sequence number order may be violated → deduplication errors
C) Kafka doesn't support > 5 parallel connections
D) This is a TCP stack limitation

**Answer:** B —
Idempotency is guaranteed only if sequence numbers arrive in the correct order.
With > 5 in-flight requests, out-of-order delivery is possible → broker cannot correctly deduplicate.

---

**Question 9:**
In a production cluster (3 brokers, RF=3, min.insync.replicas=2) using `acks=all`.
One broker is unavailable. What happens to the producer?

A) Producer gets an error — ISR < 3
B) Producer continues working — ISR = 2 ≥ min.insync.replicas
C) Kafka automatically downgrades to acks=1
D) Producer starts buffering messages until broker is restored

**Answer:** B —
min.insync.replicas=2, ISR=2 (two brokers still alive) ≥ minimum.
Producer receives acknowledgment from 2 ISR → `acks=all` satisfied → OK.

---

**Question 10:**
Which characteristic is NOT a benefit of increasing `linger.ms`?

A) Higher throughput due to fewer network requests
B) Better compression due to larger batch
C) Lower latency for the first message
D) Less network load under high load

**Answer:** C —
Increasing `linger.ms` **increases** latency: the first message waits for accumulation.
All other points (A, B, D) — genuine advantages of batching.