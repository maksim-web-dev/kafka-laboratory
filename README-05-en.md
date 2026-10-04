# Branch 05 — Multiple Consumer Groups

## `branch05_multiple_consumer_groups` — what we learn

- **Two independent consumer groups read the same topic simultaneously** — neither interferes with the other.
- **Independent offsets** — each group stores its reading progress in `__consumer_offsets`.
- **Kafka does not delete messages after reading** — unlike queues like RabbitMQ.
- **Fan-out pattern** — one producer, unlimited consumer groups, no data copies.
- **Consumer Lag** — the difference between a group's current offset and the end of the partition (LOG-END-OFFSET).
- **auto-offset-reset: earliest** — a new group reads the topic from the very beginning.
- **Replay** — resetting offset via `--reset-offsets --to-earliest` to re-read all events.
- **New `analytics-service`** — listens to `05.orders.created` and accumulates statistics in memory.
- **`ConcurrentHashMap` + `AtomicInteger`** — thread-safe accumulators for parallel consumer threads.
- **`kafka-consumer-groups --describe`** — CLI tool for monitoring lag of each group.

## What changed compared to branch04

- Instead of 3 instances of `notification-service` — now **1 instance** (simplified for demo).
- New service **`analytics-service`** added with its own consumer group `analytics-service-group`.
- `analytics-service` subscribes only to `05.orders.created` — does not listen to `05.orders.cancelled`.
- New REST endpoints: `GET /api/analytics/stats` and `GET /api/analytics/stats/user/{userId}`.
- `analytics-service` uses `auto-offset-reset: earliest` — reads from the beginning of the topic on first launch.
- Port `analytics-service`: **8084** (according to the standardized scheme).
- Topic names changed to `05.orders.*` (according to branch numbering).

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  order-service :8081                                            │
│  POST /api/orders  →  KafkaTemplate.send(key=userId)            │
└───────────────────────┬─────────────────────────────────────────┘
                        │  key=userId
                        ▼
        ╔═══════════════════════════════╗
        ║   05.orders.created           ║
        ║   partitions: 3               ║
        ║   retention: 7 days           ║
        ╠══════════╦══════════╦═════════╣
        ║ Part. 0  ║ Part. 1  ║ Part. 2 ║
        ╚═════╤════╩═════╤════╩════╤════╝
              │          │         │
    ┌─────────┴──────────┴─────────┴──────────┐
    │                                          │
    ▼                                          ▼
notification-service-group          analytics-service-group
notification-service-b05 :8082      analytics-service-b05 :8084
→ sends email                       → counts statistics
offset: independent ✓               offset: independent ✓
```

```
__consumer_offsets (Kafka internal topic):

notification-service-group | 05.orders.created | partition 0 → offset 15
notification-service-group | 05.orders.created | partition 1 → offset 12
notification-service-group | 05.orders.created | partition 2 → offset 18

analytics-service-group    | 05.orders.created | partition 0 → offset 8   ← lagging!
analytics-service-group    | 05.orders.created | partition 1 → offset 12
analytics-service-group    | 05.orders.created | partition 2 → offset 18
```

### Topics and their configuration

- `05.orders.created` → partitions: 3, retention: 7 days (604 800 000 ms),
  read by two groups: `notification-service-group` and `analytics-service-group`.
- `05.orders.cancelled` → partitions: 1, retention: 7 days,
  read only by `notification-service-group`.

## Key concepts of this branch

### Multiple Consumer Groups — independent reading of the same topic

Any number of consumer groups can read the same topic independently of each other.
Kafka stores messages regardless of who has read them and how many times.
Each group "sees" the topic as if it were the only reader.

```
Producer → 05.orders.created:
  offset 0: OrderCreated{userId="user-42"}
  offset 1: OrderCreated{userId="user-99"}
  offset 2: OrderCreated{userId="user-42"}

notification-service-group (partition 1):
  reads offset 0 ✓ → email sent
  reads offset 1 ✓ → email sent
  committed offset = 2

analytics-service-group (same partition 1):
  reads offset 0 ✓ → counted
  reads offset 1 ✓ → counted
  committed offset = 2
  (independent of notification)
```

> Reading by one group **does not advance** the offset of another group — each stores its own progress.

### Independent offsets in `__consumer_offsets`

Kafka stores committed offset **separately for each (group, topic, partition) triple**.
That means for 2 groups and 3 partitions — 6 independent records in `__consumer_offsets`.

```
record key: (group-id, topic-name, partition)
value:      committed-offset

("notification-service-group", "05.orders.created", 0) → 15
("notification-service-group", "05.orders.created", 1) → 12
("analytics-service-group",    "05.orders.created", 0) → 8
("analytics-service-group",    "05.orders.created", 1) → 12
```

### Consumer Lag — group lag

**Lag** shows how many messages a consumer group has not yet read.

```
LAG = LOG-END-OFFSET − COMMITTED-OFFSET

Example:
  partition 0: LOG-END = 10, committed = 8  →  LAG = 2
  partition 1: LOG-END = 12, committed = 12 →  LAG = 0
```

- LAG = 0 — group is real-time, messages are processed immediately.
- LAG > 0 — group is lagging: service stopped, or processing slower than messages arrive.
- Monitoring lag — key health indicator for consumers in production.

### Fan-out pattern — one producer, N consumer groups

```
order-service
    │  publish once
    ▼
05.orders.created
    ├── notification-service-group  → email notifications
    ├── analytics-service-group     → statistics
    └── (future) audit-service-group → audit
```

Producer publishes messages **once**.
Kafka stores data and allows any number of groups to read it independently.
No data duplication, no coordination between consumers.

### auto-offset-reset: earliest vs latest

The `auto-offset-reset` setting determines where a group starts reading
if there is **no committed offset yet** for it (first launch or new group).

```yaml
# analytics-service/application.yml
spring:
  kafka:
    consumer:
      auto-offset-reset: earliest  # read from the beginning of the topic
```

- `earliest` — read all messages from the beginning (from offset 0 of each partition).
  Needed for analytics/audit where a complete picture is important.
- `latest` — read only new messages that arrive **after** startup.
  Suitable for real-time services that don't need old data.

> `notification-service` also uses `earliest` — to not miss
> any order even if the service starts later than the producer.

### Replay — re-reading all events

`--reset-offsets --to-earliest` resets the committed offset of a group to the beginning of the topic.
On next launch the group re-reads **all** stored messages.

```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group analytics-service-group \
  --topic 05.orders.created \
  --reset-offsets --to-earliest --execute
```

This is useful when:
- launching a new service that needs to process old events.
- a logic error occurred and data needs to be reprocessed.
- testing: need to clear state and start from scratch.

### ConcurrentHashMap + AtomicInteger — thread-safe statistics

Spring Kafka by default runs **multiple threads** for parallel partition processing.
Therefore `OrderAnalyticsListener` uses thread-safe structures:

```kotlin
private val totalOrders = AtomicInteger(0)
private val totalRevenueCents = AtomicLong(0)
private val ordersByUser = ConcurrentHashMap<String, AtomicInteger>()

@KafkaListener(topics = ["05.orders.created"], groupId = "analytics-service-group")
fun handleOrderCreated(record: ConsumerRecord<String, OrderCreatedEvent>) {
    val event = record.value()
    val total = totalOrders.incrementAndGet()
    val amountCents = (event.totalAmount * 100).toLong()
    totalRevenueCents.addAndGet(amountCents)
    ordersByUser.computeIfAbsent(event.userId) { AtomicInteger(0) }.incrementAndGet()

    log.info("[ANALYTICS] Order counted → userId={}, partition={}, offset={}, totalOrders={}",
        event.userId, record.partition(), record.offset(), total)
}
```

`AtomicInteger.incrementAndGet()` and `AtomicLong.addAndGet()` — atomic operations,
safe for concurrent access from multiple threads without explicit locks.

## How to run

```bash
docker compose -f docker-compose-05.yml up --build
```

Check 5 containers:

```bash
docker compose -f docker-compose-05.yml ps
# kafka, kafka-ui, order-service-b05
# notification-service-b05, analytics-service-b05
```

## How to test

### 1. Send several orders (Linux)

```bash
for user in user-01 user-02 user-01 user-03 user-02 user-01; do
  curl -s -X POST http://localhost:8081/api/orders \
    -H "Content-Type: application/json" \
    -d "{\"userId\":\"$user\",\"product\":\"Book\",\"quantity\":1,\"totalAmount\":25.00}"
done
```

**Windows (PowerShell):**

```powershell
foreach ($user in @("user-01","user-02","user-01","user-03","user-02","user-01")) {
  Invoke-RestMethod -Method POST -Uri "http://localhost:8081/api/orders" `
    -ContentType "application/json" `
    -Body "{`"userId`":`"$user`",`"product`":`"Book`",`"quantity`":1,`"totalAmount`":25.00}"
}
```

### 2. Verify that both groups received all events

```bash
# notification-service — email notification counter
curl http://localhost:8082/api/notifications/count
# {"instanceId":"notification-service-1","orders_created":6,"orders_cancelled":0,"total":6}

# analytics-service — overall statistics
curl http://localhost:8084/api/analytics/stats
```

Expected analytics response:

```json
{
  "consumerGroup": "analytics-service-group",
  "totalOrders": 6,
  "totalRevenue": "150.00",
  "uniqueUsers": 3,
  "ordersByUser": { "user-01": 3, "user-02": 2, "user-03": 1 },
  "revenueByUser": { "user-01": "75.00", "user-02": "50.00", "user-03": "25.00" }
}
```

### 3. Per-user statistics

```bash
curl http://localhost:8084/api/analytics/stats/user/user-01
# {"userId":"user-01","orders":3,"revenue":"75.00"}
```

**Windows (PowerShell):**

```powershell
Invoke-RestMethod http://localhost:8084/api/analytics/stats/user/user-01
```

### 4. Main demo scenario: stop analytics, send messages, start again

```bash
# Step 1: Stop analytics-service
docker stop analytics-service-b05
echo "Analytics stopped"

# Step 2: Send 5 new orders (notification will receive, analytics will not)
for i in 1 2 3 4 5; do
  curl -s -X POST http://localhost:8081/api/orders \
    -H "Content-Type: application/json" \
    -d "{\"userId\":\"user-missed\",\"product\":\"Missed Order #$i\",\"quantity\":1,\"totalAmount\":10.00}"
done

# Step 3: Notification received all messages
curl http://localhost:8082/api/notifications/count
# total: 11

# Step 4: Start analytics again
docker start analytics-service-b05

# Step 5: After 5-10 seconds analytics catches up
curl http://localhost:8084/api/analytics/stats
# totalOrders: 11  ← caught up on all
# ordersByUser.user-missed: 5  ← processed missed ones
```

### 5. Check lag via CLI

```bash
# While analytics is stopped — lag > 0:
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group analytics-service-group

# GROUP                    TOPIC              PARTITION  OFFSET  LOG-END  LAG
# analytics-service-group  05.orders.created  0          2       4        2
# analytics-service-group  05.orders.created  1          3       5        2
# analytics-service-group  05.orders.created  2          1       3        2

# notification-service is not lagging:
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group notification-service-group
# LAG = 0 for all partitions
```

### 6. Kafka UI — two consumer groups

Open: `http://localhost:8080`

- `Consumer Groups` → two entries:
  - `notification-service-group` (lag = 0, 1 member)
  - `analytics-service-group` (lag = 0 or > 0 if stopped)
- Compare offsets of each group — they are independent.
- `Topics` → `05.orders.created` → `Consumers` → both groups visible in one tab.

### 7. Replay: reset analytics offset to the beginning

```bash
# Stop analytics (group cannot read while open)
docker stop analytics-service-b05

# Reset offset to the oldest message
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group analytics-service-group \
  --topic 05.orders.created \
  --reset-offsets --to-earliest --execute

# Start analytics again — it will re-read ALL messages from the beginning
docker start analytics-service-b05
```

## How it works internally

### Producer (changes from branch04)

Topic names changed from `04.orders.*` to `05.orders.*` — just refactoring:

```kotlin
// OrderService.kt
private val topicCreated = "05.orders.created"
private val topicCancelled = "05.orders.cancelled"

fun createOrder(...): OrderCreatedEvent {
    kafkaTemplate.send(topicCreated, userId, event)  // key = userId (unchanged)
}
```

### Consumer — notification-service (unchanged from branch04)

Listens to `05.orders.created` with groupId `notification-service-group`.
Logic and structure — identical to branch04, just one instance:

```kotlin
@KafkaListener(topics = ["05.orders.created"], groupId = "notification-service-group")
fun handleOrderCreated(record: ConsumerRecord<String, OrderCreatedEvent>) {
    log.info("║  Instance  : {}", instanceId)
    log.info("║  Key: {}  →  Partition: {}  Offset: {}", record.key(), record.partition(), record.offset())
    log.info("║  → Email sent to user {}", event.userId)
}
```

### Consumer — analytics-service (NEW)

Different group ID, `earliest` offset reset, thread-safe accumulators:

```kotlin
// OrderAnalyticsListener.kt
@KafkaListener(topics = ["05.orders.created"], groupId = "analytics-service-group")
fun handleOrderCreated(record: ConsumerRecord<String, OrderCreatedEvent>) {
    val event = record.value()
    totalOrders.incrementAndGet()
    totalRevenueCents.addAndGet((event.totalAmount * 100).toLong())
    ordersByUser.computeIfAbsent(event.userId) { AtomicInteger(0) }.incrementAndGet()

    log.info("[ANALYTICS] Order counted → userId={}, partition={}, offset={}, totalOrders={}",
        event.userId, record.partition(), record.offset(), totalOrders.get())
}
```

```yaml
# analytics-service/application.yml
server:
  port: 8084

spring:
  kafka:
    consumer:
      group-id: analytics-service-group   # ← different group than notification
      auto-offset-reset: earliest          # ← read from beginning on first start
```

## Project structure (changes relative to branch04)

```
kafka-laboratory/
├── docker-compose-05.yml               ← 1 notification (not 3), + analytics-service
├── branch05_multiple_consumer_groups/
│   ├── analytics-service/              ← NEW service
│   │   ├── Dockerfile
│   │   ├── build.gradle.kts
│   │   └── src/main/kotlin/com/kafkalab/analytics/
│   │       ├── AnalyticsServiceApplication.kt
│   │       ├── controller/
│   │       │   └── AnalyticsController.kt    ← /stats, /stats/user/{id}
│   │       ├── listener/
│   │       │   └── OrderAnalyticsListener.kt ← analytics-service-group
│   │       └── model/
│   │           └── OrderCreatedEvent.kt
│   │   └── src/main/resources/
│   │       └── application.yml               ← group-id: analytics-service-group
│   ├── notification-service/           ← 1 instance (simplified from branch04)
│   │   └── src/main/resources/
│   │       └── application.yml               ← group-id: notification-service-group
│   └── order-service/                  ← topics 05.orders.*
│       └── src/main/kotlin/.../config/
│           └── KafkaTopicConfig.kt           ← 05.orders.created (3 part.)
└── README05.md
```

## What's next — branch06

- **Auto commit** (`enable.auto.commit=true`) — risks and interval configuration.
- **Manual commit** — `commitSync()` vs `commitAsync()` and when to use each.
- **Offset reset strategies** — `earliest`, `latest`, `none` and when each is appropriate.
- **At-least-once vs at-most-once** — delivery guarantees depending on commit strategy.
- **Simulating failure during processing** — what happens with uncommitted offset.

---
---

## Presentation slides (12 slides)

**Slide 1: Branch 05 — Multiple Consumer Groups**
- Apache Kafka for Certification & Production
- Two independent consumers read the same topic

**Slide 2: Agenda**
1. Problem: how do two services get the same data?
2. Multiple Consumer Groups — how it works
3. Independent offsets in `__consumer_offsets`
4. Fan-out pattern — one producer, N groups
5. Consumer Lag — monitoring lag
6. auto-offset-reset: earliest vs latest
7. Replay — resetting offset to the beginning
8. Demo: stop analytics → send → start
9. Thread-safe accumulators: AtomicInteger + ConcurrentHashMap
10. Key takeaways & CCDAK

**Slide 3: How do two services get the same data?**
- Kafka does not delete messages after reading
- Messages are stored until `retention.ms` expires (default 7 days)
- Any number of consumer groups reads the topic independently
- Each group "sees" the topic from its own offset

**Slide 4: Multiple Consumer Groups — architecture**
- ASCII diagram: order-service → 05.orders.created → notification-group + analytics-group
- Two independent reading streams
- notification: sends email
- analytics: counts statistics

**Slide 5: Independent offsets**
- `__consumer_offsets` — Kafka internal topic
- Key: (group-id, topic-name, partition)
- notification offset and analytics offset — completely independent
- Reading one does not affect the other's progress

**Slide 6: Consumer Lag**
- LAG = LOG-END-OFFSET − COMMITTED-OFFSET
- LAG = 0: group is real-time
- LAG > 0: group is lagging (stopped or slow)
- Key production metric: alert if LAG exceeds threshold

**Slide 7: auto-offset-reset: earliest vs latest**
- `earliest` — read all messages from the beginning (analytics, audit)
- `latest` — read only new messages (real-time UI, notifications)
- Applied only on FIRST group launch (or after replay)

**Slide 8: Fan-out Pattern**
- One producer publishes once
- Number of groups is unlimited
- No data duplication in the topic
- Each group independently decides what to do with data

**Slide 9: Demo — Stop → Send → Start**
- Stop analytics-service-b05
- Send 5 orders (notification receives, analytics does not)
- Start analytics again
- Analytics catches up on missed events thanks to stored offset

**Slide 10: Replay**
- `--reset-offsets --to-earliest` resets offset to the beginning
- Group re-reads all stored messages
- Use cases: new service, logic error, testing
- Kafka stores data — replay always possible within retention

**Slide 11: Thread-safe accumulators**
- `AtomicInteger` / `AtomicLong` — atomic operations without locks
- `ConcurrentHashMap` — parallel access from multiple threads
- Spring Kafka can process multiple partitions in parallel
- Avoid race conditions when counting statistics

**Slide 12: Key Takeaways**
- Independent offsets guarantee isolation between groups
- Consumer Lag — main metric for production monitoring
- Fan-out without data copying — Kafka's key advantage over queues
- Replay possible within retention.ms
- auto-offset-reset: earliest vs latest — choice depends on business requirements

## Presentation script

**Slide 1:**
Welcome to the fifth branch of our course.
Previously we covered how one consumer group reads a topic.
Today we learn how to give multiple independent services access to the same data simultaneously.

**Slide 2:**
We'll cover ten topics: from the basic principle of multiple groups to thread-safe implementation.
The main insight: Kafka is not a queue with one reader, but an event log for any number of consumers.

**Slide 3:**
First question: why can two services receive the same messages?
Kafka does not delete messages after reading — this is the key difference from RabbitMQ or ActiveMQ.
Data is stored until retention.ms expires — 7 days by default.
So any number of consumer groups can read the topic from their own position.

**Slide 4:**
In our architecture, order-service publishes orders to `05.orders.created`.
notification-service-group reads these messages and sends email.
analytics-service-group reads the same messages and counts statistics.
Neither knows about the existence of the other — complete isolation.

**Slide 5:**
Kafka stores committed offset for each (group, topic, partition) combination in the internal `__consumer_offsets` topic.
notification offset 15 and analytics offset 8 for the same partition — two independent numbers.
Analytics may lag, but this has absolutely no effect on notification.

**Slide 6:**
Consumer Lag is the difference between the partition end and where the group currently is.
LAG zero means the group processes messages faster than they arrive.
LAG greater than zero — the group is lagging: either slow processing or service stopped.
In production: alert if lag exceeds a threshold level.

**Slide 7:**
`auto-offset-reset` is an important setting for new groups.
`earliest`: read from the very beginning of the topic — suitable for analytics, audit, data-warehouse.
`latest`: read only new messages — suitable for real-time services where old data is irrelevant.
Analytics uses `earliest` — to not miss any order.

**Slide 8:**
Fan-out is a pattern where one source generates data for many consumers.
In Kafka, the producer publishes once, and data is stored.
Any new consumer group connects and reads whatever it needs.
No need for additional copies or routing logic.

**Slide 9:**
Our demo has three steps.
Stop analytics-service — it no longer reads the topic.
Send 5 orders — notification receives and processes them.
Start analytics again — it catches up on all missed messages from the stored offset.

**Slide 10:**
Replay — restoring state from the beginning of the topic.
Reset committed offset via CLI command `--reset-offsets --to-earliest`.
On next launch the group re-reads all stored messages.
Useful when there's a logic error or launching a new analytics service on old data.

**Slide 11:**
Spring Kafka can process multiple partitions in parallel — each in its own thread.
If multiple threads update counters simultaneously, a regular int or HashMap causes a race condition.
We use `AtomicInteger.incrementAndGet()` and `ConcurrentHashMap.computeIfAbsent()` — operations are atomic without explicit locks.

**Slide 12:**
Key takeaway: Kafka is an event log, not a queue.
Multiple consumer groups — the main difference from traditional brokers.
Consumer lag — the first metric monitored in production.
Replay makes Kafka a unique tool for event-driven architectures.

## Test Questions (up to 10 questions)

**Question 1:**
Two consumer groups (`notification-group` and `analytics-group`) read the same topic.
`notification-group` is at offset 100.
What will happen to the offset of `analytics-group`?

A) It will also move to offset 100
B) It will remain unchanged — each group has its own offset
C) Kafka will delete messages up to offset 100
D) `analytics-group` will receive a conflict error

**Answer:** B —
Each consumer group stores its own committed offset independently.
Reading by one group does not affect the progress of another.

---

**Question 2:**
What does Kafka store in the internal `__consumer_offsets` topic?

A) The actual messages from all topics
B) Committed offset for each (group, topic, partition) combination
C) List of active consumer instances
D) Broker configuration

**Answer:** B —
`__consumer_offsets` stores key-value pairs where the key is `(group-id, topic-name, partition)`,
and the value is the last committed offset for that group in that partition.

---

**Question 3:**
Consumer Lag of a partition = 15.
LOG-END-OFFSET of this partition = 20.
What is the committed offset of the consumer group?

A) 5
B) 15
C) 20
D) 35

**Answer:** A —
LAG = LOG-END-OFFSET − COMMITTED-OFFSET → 15 = 20 − COMMITTED → COMMITTED = 5.

---

**Question 4:**
`analytics-service` starts for the first time (no committed offset yet for it).
Configuration: `auto-offset-reset: earliest`.
What behavior will it have?

A) It will read only new messages after startup
B) It will read all stored messages from the beginning of the topic
C) It will generate an error due to missing offset
D) It will start from the middle of the topic

**Answer:** B —
`auto-offset-reset: earliest` instructs starting from the oldest available offset
(offset 0 or the first that hasn't been deleted due to retention).

---

**Question 5:**
Which `auto-offset-reset` setting is appropriate for a real-time notification service
that doesn't need old events?

A) `earliest`
B) `latest`
C) `none`
D) `reset`

**Answer:** B —
`latest` means start from the end of the topic — read only new messages after startup.
`earliest` is suitable for analytics and audit.
`none` throws an exception if there is no committed offset — used in special scenarios.

---

**Question 6:**
Which command resets a group's committed offset to the beginning of the topic?

A) `kafka-consumer-groups --reset-offsets --to-latest`
B) `kafka-consumer-groups --reset-offsets --to-earliest --execute`
C) `kafka-topics --delete-offsets`
D) `kafka-consumer-groups --clear-offsets`

**Answer:** B —
`--reset-offsets --to-earliest --execute` resets the committed offset to the oldest message.
Without `--execute` the command only shows what would be changed (dry-run).

---

**Question 7:**
Why does analytics-service use `ConcurrentHashMap` instead of a regular `HashMap`?

A) `ConcurrentHashMap` is faster for a single thread
B) Spring Kafka can process multiple partitions in parallel threads
C) Kafka requires thread-safe structures by specification
D) `HashMap` doesn't support String keys

**Answer:** B —
Spring Kafka can assign multiple partitions to one consumer instance and process them in parallel.
Without thread-safe structures, a race condition is possible when updating counters concurrently.

---

**Question 8:**
What is the main difference between Kafka and a queue (RabbitMQ, ActiveMQ) from the perspective of multiple consumers?

A) Kafka supports more than one consumer
B) Kafka does not delete messages after reading — they are available to all groups
C) Kafka automatically sends copies to each consumer
D) Kafka has no consumer groups

**Answer:** B —
In traditional queues a message is deleted after delivery to one consumer.
Kafka stores messages until retention expires — so any number of groups reads independently.

---

**Question 9:**
A company launches a new audit-service and wants it to read all orders from the last 7 days.
The topic `orders.created` has retention.ms = 604800000 (7 days).
What steps are needed?

A) Launch audit-service with `auto-offset-reset: earliest` — it will read everything itself
B) Copy messages manually from another service
C) Stop Kafka and restart with new settings
D) Subscribe audit-service to the existing notification-service-group

**Answer:** A —
The new consumer group has no committed offset.
`auto-offset-reset: earliest` will make it start from offset 0.
Kafka stores all messages within retention, so audit will receive all 7 days.

---

**Question 10:**
analytics-service is stopped for 30 minutes.
During this time notification-service received 200 new orders (analytics LAG = 200).
What will happen when analytics-service starts again?

A) It will skip 200 messages — they are already "old"
B) It will catch up on all 200 missed messages from the stored offset
C) Kafka will delete the missed messages to synchronize offsets
D) analytics-service will generate an error due to large LAG

**Answer:** B —
Kafka stores messages regardless of whether any group has read them.
On restart, analytics-service will continue reading from the last committed offset
and catch up on all 200 missed messages.