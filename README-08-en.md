# Branch 08 — Consumer Configuration

## `branch08_consumer_configuration` — what we learn

- **CooperativeStickyAssignor** — incremental rebalance: only "migrating" partitions are reassigned.
- **EAGER vs COOPERATIVE** — the difference between stop-the-world and incremental rebalance.
- **Static Group Membership** — `group.instance.id` to reduce rebalance frequency on restart.
- **`onPartitionsRevoked` in COOPERATIVE** — called only for the partitions being released.
- **`max.poll.records`** — limit the number of records per single `poll()`.
- **`max.poll.interval.ms`** — max time between two `poll()` calls before broker kicks the consumer out.
- **`session.timeout.ms`** — time without a heartbeat after which the broker declares the consumer dead.
- **`heartbeat.interval.ms`** — heartbeat send frequency (rule: < session.timeout / 3).
- **Graceful shutdown** — `server.shutdown=graceful` + commit before exit.
- **`/api/notifications/partitions`** — new endpoint to view assigned partitions.

## What changed compared to branch07

- Restored 2 instances of `notification-service` (1-b08 on port 8082, 2-b08 on port 8083).
- `partition.assignment.strategy` changed to `CooperativeStickyAssignor`.
- `group.instance.id` configured for each `@KafkaListener` — static membership.
- Added `max.poll.records=10`, `max.poll.interval.ms=300000`, `session.timeout.ms=45000`.
- `server.shutdown=graceful` and `lifecycle.timeout-per-shutdown-phase=30s`.
- New endpoint `/api/notifications/partitions` — shows assigned partitions.
- New endpoint `/api/notifications/config` — shows current consumer configuration.
- Topic names changed to `08.orders.*`.

## Architecture

```
order-service-b08 :8081
  POST /api/orders → kafkaTemplate.send(topicCreated, userId, event)
          │
     08.orders.created (3 partitions)
          │
    ┌─────┴────────────────────────────────────┐
    │ notification-service-group (COOPERATIVE)  │
    │                                           │
    │  notification-1-b08 :8082                 │
    │    group.instance.id:                     │
    │      "notification-1-orders-created"      │
    │    assigned: [partition 0, partition 1]   │
    │                                           │
    │  notification-2-b08 :8083                 │
    │    group.instance.id:                     │
    │      "notification-2-orders-created"      │
    │    assigned: [partition 2]                │
    └───────────────────────────────────────────┘

COOPERATIVE rebalance (when notification-2 stops):
  REVOKED:  notification-2 → [partition 2]        ← only this partition
  ASSIGNED: notification-1 → [partition 2]        ← notification-1 keeps [0,1] without interruption
  (partition 0 and 1 are NOT touched — they stay with notification-1)
```

### Topics and their configuration

- `08.orders.created` → partitions: 3, retention: 7 days,
  read by `notification-service-group` with CooperativeStickyAssignor.
- `08.orders.cancelled` → partitions: 1, retention: 7 days.

## Key Concepts of This Branch

### EAGER vs COOPERATIVE Rebalance

**EAGER (RangeAssignor — branch04):**

```
Event: notification-2 joins the group
  1. ALL consumers: REVOKED all partitions → stop reading
  2. Group coordinator: recalculates assignments
  3. ALL consumers: ASSIGNED new partitions → resume reading

Stop-the-world: the entire consumer group stops reading during rebalance (seconds)
```

**COOPERATIVE (CooperativeStickyAssignor — branch08):**

```
Event: notification-2 joins the group
  Round 1:
    consumers report what they have: notification-1 → [0,1,2]
    coordinator determines: partition 2 will move to notification-2
    notification-1 REVOKED: only [partition 2] ← not [0] and not [1]
  Round 2:
    notification-2 ASSIGNED: [partition 2]
    notification-1 keeps reading [0,1] without any interruption ✓
```

Logs with COOPERATIVE:

```
# EAGER (branch04):
[REBALANCE] REVOKED:  [orders.created[0], orders.created[1], orders.created[2]]
[REBALANCE] ASSIGNED: [orders.created[0], orders.created[1]]

# COOPERATIVE (branch08):
[COOPERATIVE] notification-1 REVOKED:  [orders.created[2]]   ← only this one
[COOPERATIVE] notification-2 ASSIGNED: [orders.created[2]]
```

### Static Group Membership

Without `group.instance.id` (dynamic): every restart → 2 rebalances (disconnect + reconnect).

```
Dynamic membership:
  notification-2 restarts
  → broker: "member leaves" → rebalance (partition 2 moves to notification-1)
  → broker: "new member joins" → rebalance again (partition 2 returns)
  = 2 rebalances, ~session.timeout.ms downtime

Static membership (group.instance.id):
  notification-2 restarts
  → broker: "instance notification-2-orders-created offline"
  → broker waits session.timeout.ms (45s) before declaring dead
  → if notification-2 returns BEFORE session.timeout → NO rebalance ✓
  = 0 rebalances on fast restart
```

Each `@KafkaListener` gets a unique instance ID:

```kotlin
// application.yml
instance:
  id: ${INSTANCE_ID:notification-1}

// OrderEventListener.kt
@KafkaListener(
    topics = ["08.orders.created"],
    groupId = "notification-service-group",
    properties = [
        "group.instance.id=\${instance.id}-orders-created"  // unique per listener
    ]
)
fun handleOrderCreated(...) { ... }
```

### max.poll.records and max.poll.interval.ms

```
max.poll.records=10:
  Consumer reads ≤ 10 messages per poll()
  Fewer messages → less batch processing time → lower risk of exceeding max.poll.interval

max.poll.interval.ms=300000 (5 min):
  If time between two poll() > 5 min → broker: "consumer dropped" → rebalance
  Common problem: heavy processing of one batch > max.poll.interval
  Solution: reduce max.poll.records or increase max.poll.interval.ms
```

### session.timeout.ms and heartbeat.interval.ms

```
heartbeat.interval.ms=15000 (15s):
  Consumer sends a heartbeat every 15s (in a background thread, independent of poll)
  Rule: heartbeat.interval < session.timeout / 3

session.timeout.ms=45000 (45s):
  Broker declares consumer dead if no heartbeat received for 45s
  Lower = faster failure detection, but risk of false positives during GC pause or high load
  Higher = slower detection, but more stable under temporary delays
```

### Graceful shutdown

```yaml
# notification-service/application.yml
server:
  shutdown: graceful               # Jetty/Tomcat finishes active requests
  lifecycle:
    timeout-per-shutdown-phase: 30s  # 30s maximum to finish
```

On SIGTERM:

```
1. Spring: reject new HTTP requests
2. Spring Kafka: finish the current batch
3. Spring Kafka: commitSync() — commit the offset
4. COOPERATIVE: REVOKED assigned partitions → other consumers receive them
5. Graceful exit (no message loss)
```

## How to Run

```bash
docker compose -f docker-compose-08.yml up --build
```

Verify 5 containers:

```bash
docker compose -f docker-compose-08.yml ps
# kafka, kafka-ui, order-service-b08
# notification-service-1-b08 (port 8082)
# notification-service-2-b08 (port 8083)
```

## How to Test

### 1. Check partition distribution

```bash
curl http://localhost:8082/api/notifications/partitions
# {"instanceId":"notification-1","assignedPartitions":["orders.created[0]","orders.created[1]"],"count":2}

curl http://localhost:8083/api/notifications/partitions
# {"instanceId":"notification-2","assignedPartitions":["orders.created[2]"],"count":1}
```

### 2. View current consumer configuration

```bash
curl http://localhost:8082/api/notifications/config
```

Expected response:

```json
{
  "instanceId": "notification-1",
  "groupId": "notification-service-group",
  "staticMemberId_created": "notification-1-orders-created",
  "assignmentStrategy": "CooperativeStickyAssignor",
  "maxPollRecords": 10,
  "maxPollIntervalMs": 300000,
  "sessionTimeoutMs": 45000,
  "heartbeatIntervalMs": 15000,
  "ackMode": "MANUAL_IMMEDIATE",
  "gracefulShutdown": true
}
```

### 3. Main demo scenario: COOPERATIVE rebalance

```bash
# Step 1: Send 6 orders
for i in 1 2 3 4 5 6; do
  curl -s -X POST http://localhost:8081/api/orders \
    -H "Content-Type: application/json" \
    -d "{\"userId\":\"user-0$i\",\"product\":\"Book\",\"quantity\":1,\"totalAmount\":25.00}"
done

# Step 2: Graceful shutdown of notification-service-2
docker stop notification-service-2-b08

# notification-service-2 logs (graceful):
# [GRACEFUL] notification-2: finishing batch, committing offsets...
# [COOPERATIVE] notification-2 REVOKED: [orders.created[2]]

# notification-service-1 logs (only new partitions, [0],[1] untouched):
# [COOPERATIVE] notification-1 ASSIGNED: [orders.created[2]]

# Step 3: notification-1 now reads all 3 partitions
curl http://localhost:8082/api/notifications/partitions
# {"instanceId":"notification-1","assignedPartitions":["orders.created[0]","orders.created[1]","orders.created[2]"],"count":3}

# Step 4: Start notification-2 again
docker start notification-service-2-b08
```

**Windows (PowerShell):**

```powershell
for ($i=1; $i -le 6; $i++) {
  Invoke-RestMethod -Method POST -Uri http://localhost:8081/api/orders `
    -ContentType "application/json" `
    -Body "{`"userId`":`"user-0$i`",`"product`":`"Book`",`"quantity`":1,`"totalAmount`":25.00}"
}
```

### 4. Verify static membership via CLI

```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group notification-service-group

# GROUP                       TOPIC              PARTITION  CONSUMER-ID
# notification-service-group  orders.created  0  notification-1-orders-created-...
# notification-service-group  orders.created  1  notification-1-orders-created-...
# notification-service-group  orders.created  2  notification-2-orders-created-...
```

CONSUMER-ID starts with `group.instance.id` → confirms static membership.

### 5. Kafka UI

Open: `http://localhost:8080`

- `Consumer Groups` → `notification-service-group` → Members.
- See `notification-1-orders-created` and `notification-2-orders-created` as member IDs.
- Stop notification-2: observe the delay before rebalance (session.timeout = 45s).

## How It Works Internally

### Producer (unchanged from branch07)

Production-safe configuration: `acks=all`, `enable.idempotence=true`, `linger.ms=20`.

### Consumer — notification-service (key changes)

```yaml
# notification-service/application.yml — branch08
server:
  shutdown: graceful                     # ← NEW: graceful shutdown
  lifecycle:
    timeout-per-shutdown-phase: 30s

spring:
  kafka:
    consumer:
      enable-auto-commit: false
      max-poll-records: 10               # ← NEW
      properties:
        partition.assignment.strategy: org.apache.kafka.clients.consumer.CooperativeStickyAssignor  # ← NEW
        max.poll.interval.ms: 300000     # ← NEW
        session.timeout.ms: 45000        # ← NEW
        heartbeat.interval.ms: 15000     # ← NEW
    listener:
      ack-mode: manual_immediate
```

```kotlin
// OrderEventListener.kt — static membership per listener
@KafkaListener(
    topics = ["08.orders.created"],
    groupId = "notification-service-group",
    properties = ["group.instance.id=\${instance.id}-orders-created"]  // ← NEW
)
fun handleOrderCreated(...) { ... }

// ConsumerSeekAware for logging rebalance events:
override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
    log.info("[COOPERATIVE] {} REVOKED: {}", instanceId, partitions)
    // commitSync() before exit
}

override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
    log.info("[COOPERATIVE] {} ASSIGNED: {}", instanceId, partitions)
}
```

## Project Structure (changes from branch07)

```
kafka-laboratory/
├── docker-compose-08.yml                     ← 2 notification instances, ports 8082/8083
├── branch08_consumer_configuration/
│   ├── notification-service/
│   │   └── src/main/resources/
│   │       └── application.yml               ← COOPERATIVE, static membership, poll tuning, graceful
│   │   └── src/main/kotlin/.../listener/
│   │       └── OrderEventListener.kt         ← ConsumerSeekAware, group.instance.id, @PreDestroy
│   │   └── src/main/kotlin/.../controller/
│   │       └── NotificationController.kt     ← +/partitions, +/config
│   └── order-service/                        ← topics 08.orders.*
└── README08.md
```

## What's Next — branch09

- **`@RetryableTopic`** — non-blocking retry through separate Kafka topics.
- **Dead Letter Topic (DLT)** — final destination for messages that failed after N attempts.
- **`@DltHandler`** — DLT handler: logging, alerting, manual review.
- **Exponential backoff** — 1s → 5s before the next attempt.
- **New service `payment-service`** — replaces notification-service in the demo.

---
---

## Presentation Slides (12 slides)

**Slide 1: Branch 08 — Consumer Configuration**
- Apache Kafka for Certification & Production
- Rebalance strategies, static membership, poll tuning

**Slide 2: Agenda**
1. EAGER rebalance — the stop-the-world problem
2. COOPERATIVE (Incremental) rebalance
3. CooperativeStickyAssignor
4. Static Group Membership — group.instance.id
5. max.poll.records — protection against timeout
6. session.timeout.ms vs heartbeat.interval.ms
7. max.poll.interval.ms
8. Graceful shutdown
9. Demo: cooperative rebalance in action
10. Key takeaways & CCDAK

**Slide 3: EAGER Rebalance — Stop-the-World**
- On any group membership change: ALL consumers stop
- ALL partitions REVOKED → all ASSIGNED fresh
- Downtime = rebalance duration (seconds to a minute)
- RangeAssignor, RoundRobinAssignor — EAGER by nature

**Slide 4: COOPERATIVE Rebalance**
- Only "migrating" partitions are temporarily paused
- Two rounds: first determine what needs to move, then move it
- Other partitions keep being read without interruption
- CooperativeStickyAssignor — available since Kafka 2.4+

**Slide 5: Static Group Membership**
- Dynamic: every restart = 2 rebalances (disconnect + reconnect)
- Static: `group.instance.id` — unique per consumer
- Broker waits session.timeout before declaring dead
- Fast restart (< session.timeout) → 0 rebalances

**Slide 6: max.poll.records**
- Limits batch size per poll()
- Fewer records → less batch processing time
- Protection against exceeding max.poll.interval.ms
- Typical production value: 10–500 (depends on processing)

**Slide 7: session.timeout.ms vs heartbeat**
- Heartbeat: background thread, every heartbeat.interval.ms
- session.timeout: if no heartbeat received → consumer dead
- Rule: heartbeat.interval < session.timeout / 3
- Lower session.timeout = faster failure detection, but more false positives

**Slide 8: max.poll.interval.ms**
- Difference from session.timeout: tracks poll() not heartbeat
- If batch processing > max.poll.interval → broker kick-out → rebalance
- Common mistake: long DB operation between poll() calls
- Solution: reduce max.poll.records or increase max.poll.interval

**Slide 9: Graceful Shutdown**
- SIGTERM → Spring finishes active HTTP requests
- Spring Kafka: finishes current batch, commitSync()
- COOPERATIVE: REVOKED partitions → other consumers take them
- Without graceful: uncommitted offsets + sudden EAGER rebalance

**Slide 10: Demo — COOPERATIVE in Action**
- Start 2 instances: notification-1 → [0,1], notification-2 → [2]
- `docker stop notification-service-2-b08`
- Log: notification-2 REVOKED [2], notification-1 ASSIGNED [2]
- notification-1 never had [0] and [1] REVOKED — that's COOPERATIVE

**Slide 11: Key Takeaways**
- **CooperativeStickyAssignor** — production standard in Kafka 3.x
- **Static membership** reduces rebalances during rolling restart
- **max.poll.records** protects against max.poll.interval timeout
- **heartbeat.interval < session.timeout / 3** — always follow this rule
- **Graceful shutdown** = zero message loss during deployment

**Slide 12: What's Next — Branch 09: Error Handling & DLT**
- @RetryableTopic — non-blocking retry through Kafka topics
- Dead Letter Topic (DLT) after N failed attempts
- @DltHandler — handler for manual intervention
- Exponential backoff: 1s → 5s

## Presentation Script

**Slide 1:**
Branch eight — Consumer Configuration.
Here we move from basic concepts to production tuning of consumers.
The main topic: how to properly configure rebalance to avoid downtime during deployment.

**Slide 2:**
We'll cover ten topics: from the EAGER rebalance problem to graceful shutdown.
Key insight: rebalance is not just a technical detail — it is a direct cause of production downtime.

**Slide 3:**
EAGER rebalance — the classic approach from early Kafka versions.
Any change in group membership — a new member joins or one leaves — stops everyone.
All partitions are released and reassigned from scratch.
During this the entire consumer group stands still — this is stop-the-world.

**Slide 4:**
COOPERATIVE rebalance solves this problem.
During rebalance we only determine which partitions need to move.
Only these "migrating" partitions are temporarily paused.
All others keep reading without interruption — minimal impact on throughput.

**Slide 5:**
Static Group Membership further reduces the number of rebalances.
Without group.instance.id: every service restart is two rebalances — one when it leaves, one when it returns.
With group.instance.id: the broker identifies the instance and waits for session.timeout before declaring it dead.
If the service returns within 45 seconds — zero rebalances.

**Slide 6:**
max.poll.records limits the batch size per poll.
If processing one batch takes longer than max.poll.interval.ms — the broker kicks the consumer out and a rebalance begins.
By reducing max.poll.records we reduce batch processing time and avoid this problem.
A common production mistake: few partitions but a large batch with slow processing.

**Slide 7:**
session.timeout and heartbeat — two mechanisms for failure detection.
Heartbeat is sent in a background thread independently of poll.
If no heartbeat is received for session.timeout.ms — the broker considers the consumer dead.
The rule of thirds: heartbeat.interval must be less than session.timeout divided by three.

**Slide 8:**
max.poll.interval tracks poll rather than heartbeat.
If the gap between two polls is too large — the consumer is considered stuck.
This can happen with a long synchronous operation between poll calls — for example a database query.
Solution: reduce max.poll.records or move heavy work to async.

**Slide 9:**
Graceful shutdown is critical for zero message loss during deployment.
On SIGTERM Spring finishes the current HTTP request; Kafka finishes reading the current batch.
commitSync commits the offset — the next consumer instance starts from the correct position.
COOPERATIVE rebalance finishes the process cleanly without unnecessary delays.

**Slide 10:**
In the demo we start two instances and stop one.
We observe the logs: notification-2 REVOKED only its own partition.
notification-1 is not interrupted, it simply receives one additional partition.
This is the power of COOPERATIVE rebalance — minimal impact on system operation.

**Slide 11:**
Summary: CooperativeStickyAssignor is the production standard starting from Kafka 3.0.
Static membership further reduces rebalances during rolling restart.
max.poll.records and the session/heartbeat rule — basic tuning for any consumer.
Graceful shutdown — zero message loss during deployment.

**Slide 12:**
The next branch — Error Handling and Dead Letter Topic.
We'll explore what to do when a consumer cannot process a message.
@RetryableTopic automatically routes failed messages to retry topics.
After N attempts — the Dead Letter Topic with @DltHandler for manual intervention.

## Test Questions (up to 10 questions)

**Question 1:**
Which rebalance will occur when one of three consumers in a group using `CooperativeStickyAssignor` is stopped?

A) All three consumers REVOKED all partitions → reassignment from scratch (EAGER)
B) Only the stopped consumer's partitions REVOKED → reassigned to the rest
C) No rebalance — CooperativeStickyAssignor does not react to membership changes
D) Round 1 REVOKED all → Round 2 ASSIGNED partially

**Answer:** B —
COOPERATIVE: only the partitions that need to be moved are temporarily REVOKED.
Other consumers keep reading their partitions without interruption.

---

**Question 2:**
A consumer with `session.timeout.ms=45000` and `heartbeat.interval.ms=15000` froze due to a GC pause for 20 seconds.
What will happen?

A) Broker declares consumer dead → rebalance
B) Consumer will resume working normally — 20s < 45s
C) Kafka will automatically increase session.timeout
D) Consumer will get a CommitFailedException

**Answer:** B —
Heartbeat is sent in a background thread independently of GC.
20s < session.timeout (45s) → broker received heartbeat within the limit → OK.

---

**Question 3:**
`max.poll.records=500`. Processing one batch takes 10 minutes.
`max.poll.interval.ms=300000` (5 min). What will happen?

A) Consumer processes the batch in 10 minutes, then polls the next one
B) Broker removes consumer from the group → rebalance after 5 minutes
C) Spring Kafka automatically lowers max.poll.records to a safe value
D) Consumer gets a TimeoutException and restarts

**Answer:** B —
max.poll.interval.ms = 5 min, processing = 10 min > 5 min.
Broker did not receive a poll for 5 minutes → consumer is kicked out → rebalance.
Solution: reduce max.poll.records or increase max.poll.interval.ms.

---

**Question 4:**
What is the advantage of `group.instance.id` (static membership) over dynamic membership?

A) Consumer processes messages faster
B) Broker identifies the instance and waits session.timeout before kick-out → fewer rebalances
C) Consumer does not need heartbeat
D) Kafka does not record offsets for static members

**Answer:** B —
Static membership: the broker knows that instance X is the same consumer as before.
On restart the broker waits session.timeout instead of immediately kicking out.
If the instance returns before timeout — no rebalance occurs.

---

**Question 5:**
What is the rule for `heartbeat.interval.ms` relative to `session.timeout.ms`?

A) heartbeat.interval = session.timeout
B) heartbeat.interval < session.timeout / 3
C) heartbeat.interval = session.timeout * 2
D) heartbeat.interval > session.timeout

**Answer:** B —
Rule: `heartbeat.interval.ms < session.timeout.ms / 3`.
This guarantees at least 3 heartbeats before session.timeout.
Example: session=45s → heartbeat < 15s.

---

**Question 6:**
What is the difference between `session.timeout.ms` and `max.poll.interval.ms`?

A) session.timeout tracks heartbeat; max.poll.interval tracks poll() frequency
B) session.timeout tracks poll(); max.poll.interval tracks heartbeat
C) They are the same thing under different names
D) session.timeout is for producers; max.poll.interval is for consumers

**Answer:** A —
session.timeout: broker did not receive heartbeat for N ms → consumer dead.
max.poll.interval: consumer did not call poll() for N ms → consumer dead.
Both can independently trigger a rebalance.

---

**Question 7:**
A consumer with `CooperativeStickyAssignor` receives new partitions during rebalance.
Are its old partitions REVOKED?

A) Yes — all partitions are always REVOKED during rebalance
B) No — only if they are moving to another consumer
C) Yes — but only in the first round
D) Not always — depends on the `sticky` setting

**Answer:** B —
CooperativeStickyAssignor: a partition stays with the same consumer if possible.
REVOKED occurs only for partitions being moved to another consumer.
New partitions are simply ASSIGNED without prior REVOKED.

---

**Question 8:**
What is the advantage of `server.shutdown=graceful` for a Kafka consumer?

A) Consumer messages are not processed during shutdown
B) Consumer finishes the current batch and commitSync before exit → zero message loss
C) Kafka UI displays graceful containers differently
D) Consumer gets more time between heartbeats

**Answer:** B —
Graceful shutdown gives Spring Kafka time to finish the current batch and commit the offset.
The next consumer instance starts from the correct offset → zero duplicates during rolling restart.

---

**Question 9:**
Two consumers in a group: notification-1 → [0,1], notification-2 → [2].
notification-1 leaves the group (COOPERATIVE).
What happens?

A) All three consumers REVOKED → reassignment
B) notification-1 REVOKED [0,1] → notification-2 ASSIGNED [0,1]; [2] stays
C) notification-2 stays with [2]; no new assignment
D) Partitions [0] and [1] are left without a consumer until the next rebalance

**Answer:** B —
notification-1 leaves: partitions [0,1] become "free".
COOPERATIVE: only [0,1] REVOKED and ASSIGNED to notification-2.
[2] in notification-2 is not touched.

---

**Question 10:**
Which setting does NOT directly affect rebalance frequency in a consumer group?

A) `session.timeout.ms`
B) `group.instance.id`
C) `max.poll.records`
D) `partition.assignment.strategy`

**Answer:** C —
`max.poll.records` affects how many messages are fetched per poll, but NOT rebalance frequency directly.
(Indirect dependency: smaller batch → faster processing → lower risk of max.poll.interval timeout.)
A, B, D directly affect rebalance behavior.