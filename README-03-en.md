# Branch 03 — Message Keys

---

## `branch03_keys` — what we learn

- Message key — how the producer assigns a key and why it matters
- Partition selection algorithm: `hash(key) % numPartitions` (Murmur2)
- Determinism: the same key → always the same partition
- Ordering guarantee: within a single partition, messages are read in strict order
- `null` key → StickyPartitioner: messages are distributed across partitions without ordering guarantees
- Practical difference between `orderId` (UUID, random) and `userId` (deterministic) as keys
- New batch endpoints for comparing keyed vs non-keyed behavior
- `record.key()` in consumer — how to inspect the producer's routing decision
- `kafkaTemplate.send(...).get()` — synchronous acknowledgment for demo endpoints
- Key selection rule: when event ordering for a single entity matters → key = entity identifier

---

## What changed compared to branch02

- Key for `03.orders.created` and `03.orders.cancelled`:
  `orderId` (UUID, random) → **`userId`** (deterministic)
- New batch endpoints:
  `POST /api/orders/demo/keyed` and `POST /api/orders/demo/round-robin`
- New models:
  `BatchOrderRequest` (userId, count, product) and
  `OrderSendResult` (orderId, userId, key, partition, offset)
- Batch demo response includes real `partition` and `offset` from broker (synchronous `.get()`)
- notification-service log: now prints `Key: user-42  →  Partition: X  Offset: Y`
  (previously only Partition and Offset)

---

## Architecture

```
POST /api/orders              key = userId  ─┐
POST /api/orders/demo/keyed   key = userId  ─┤─── 03.orders.created (3 partitions)
POST /api/orders/demo/round-robin  key=null ─┘

hash(userId) % 3 → partition 0, 1, or 2

user-42  → hash("user-42") % 3 = 1  → partition 1  (always!)
user-99  → hash("user-99") % 3 = 0  → partition 0  (always!)
null key →  sticky partitioner      → round-robin   (unpredictable)

┌─────────────────┐   03.orders.created (3 partitions)   ┌───────────────────────┐
│  order-service  │  ──────────────────────────────────▶ │                       │
│  :8081          │                                       │  notification-service │
│                 │   03.orders.cancelled (1 partition)   │  :8082                │
│                 │  ──────────────────────────────────▶ │                       │
└─────────────────┘                                       └───────────────────────┘
         │                                                          │
         └─────────────────────────┬────────────────────────────────┘
                                   │
                      ┌────────────▼────────────┐
                      │      Apache Kafka        │
                      │      kafka:9092          │
                      │      (KRaft mode)        │
                      └────────────┬────────────┘
                                   │
                      ┌────────────▼────────────┐
                      │      Kafka UI            │
                      │      :8080               │
                      └─────────────────────────┘
```

### Topics and their configuration

- **`03.orders.created`** — 3 partitions, retention 7 days — new orders, key=userId
- **`03.orders.cancelled`** — 1 partition, retention 7 days — cancellations, key=userId, strict ordering
- **`03.payments.processed`** — 3 partitions, retention 7 days — reserved for branch04+
- **`03.notifications.sent`** — 1 partition, retention 1 day — reserved for branch04+

> The `03.` prefix separates this branch's topics from others when working in the same Kafka cluster.

---

## Key concepts of this branch

### How the key determines the partition

Kafka uses the Murmur2 algorithm to hash the key:

```
partition = Math.abs(murmur2(key.getBytes())) % numPartitions

key="user-42"  → murmur2 → 0x7f3a1b2c → abs % 3 = 1
key="user-42"  → same hash → same partition → ALWAYS partition 1
key="user-99"  → murmur2 → different value → abs % 3 = 0
```

**Determinism**: if the key doesn't change and the partition count doesn't change — routing is stable.
Even after restarting the broker or producer, `user-42` will always land in partition 1.

### Ordering guarantee within a partition

Kafka guarantees ordering only **within a single partition**.
If all events for a given `userId` land in the same partition — the consumer processes them in strict order:

```
Partition 0: [order-A user-99] [order-C user-99] [order-E user-99]
Partition 1: [order-B user-42] [order-D user-42] [order-F user-42]
Partition 2: [order-G user-17] [order-H user-17]

→ user-42 always: order-B → order-D → order-F (strict order)
→ across different users: parallel processing without ordering guarantees
```

### Why orderId is a bad key

At first glance `orderId` seems like a reasonable choice:
`OrderCreated` and `OrderCancelled` for the same order have the **same** `orderId`,
so `hash(orderId) % 3` gives the same partition for both.
Ordering for a specific order is preserved.

The problem arises when **one user places multiple orders**:

```
OrderCreated(orderId=order-A, userId=user-42) → hash("order-A") % 3 = 2  → partition 2
OrderCreated(orderId=order-B, userId=user-42) → hash("order-B") % 3 = 0  → partition 0
OrderCancelled(orderId=order-A, userId=user-42) → hash("order-A") % 3 = 2 → partition 2
```

`order-A` and `order-B` have different UUIDs, different hashes → different partitions.
If business logic has dependencies between orders of the same user
(credit limit, saga compensation, loyalty program) —
ordering between `order-A` and `order-B` is not guaranteed.

With `userId` as the key — all events for `user-42` regardless of order → **one partition**:

```
OrderCreated(order-A, user-42) → hash("user-42") % 3 = 1 → partition 1
OrderCreated(order-B, user-42) → hash("user-42") % 3 = 1 → partition 1
OrderCancelled(order-A, user-42) → hash("user-42") % 3 = 1 → partition 1
```

In multi-consumer scenarios (branch04+) this additionally means
all events for `user-42` are always processed by **one and the same consumer instance** —
without race conditions between instances.

### null key → StickyPartitioner

When key=null, Kafka 2.4+ uses `StickyPartitioner`:
- Picks one partition and "sticks" to it within a single batch
- After the batch is sent, moves to the next partition
- Better than round-robin (larger batch → better throughput),
  but ordering between messages is NOT guaranteed

---

## How to run

```bash
docker compose -f docker-compose-03.yml up --build
```

Check readiness:

```bash
docker compose -f docker-compose-03.yml ps
# kafka, kafka-ui, b03-order-service, b03-notification-service — Running/healthy
```

---

## How to test

### 1. Keyed demo: N orders from the same userId → one partition

```bash
curl -s -X POST http://localhost:8081/api/orders/demo/keyed \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-42","count":6,"product":"Kafka Book"}' | jq
```

On Windows:
```cmd
curl -s -X POST http://localhost:8081/api/orders/demo/keyed ^
  -H "Content-Type: application/json" ^
  -d "{\"userId\":\"user-42\",\"count\":6,\"product\":\"Kafka Book\"}"
```

Expected response (all records in **one** partition):

```json
[
  {"orderId":"uuid-1","userId":"user-42","key":"user-42","partition":1,"offset":0},
  {"orderId":"uuid-2","userId":"user-42","key":"user-42","partition":1,"offset":1},
  {"orderId":"uuid-3","userId":"user-42","key":"user-42","partition":1,"offset":2},
  {"orderId":"uuid-4","userId":"user-42","key":"user-42","partition":1,"offset":3},
  {"orderId":"uuid-5","userId":"user-42","key":"user-42","partition":1,"offset":4},
  {"orderId":"uuid-6","userId":"user-42","key":"user-42","partition":1,"offset":5}
]
```

> All 6 messages — `partition: 1`.
> The partition value is determined once: `hash("user-42") % 3 = 1`.

### 2. No-key demo: N orders → round-robin across partitions

```bash
curl -s -X POST http://localhost:8081/api/orders/demo/round-robin \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-42","count":6,"product":"Kafka Book"}' | jq
```

On Windows:
```cmd
curl -s -X POST http://localhost:8081/api/orders/demo/round-robin ^
  -H "Content-Type: application/json" ^
  -d "{\"userId\":\"user-42\",\"count\":6,\"product\":\"Kafka Book\"}"
```

Expected response (distributed across different partitions):

```json
[
  {"orderId":"uuid-1","userId":"user-42","key":null,"partition":0,"offset":0},
  {"orderId":"uuid-2","userId":"user-42","key":null,"partition":1,"offset":6},
  {"orderId":"uuid-3","userId":"user-42","key":null,"partition":2,"offset":0},
  {"orderId":"uuid-4","userId":"user-42","key":null,"partition":0,"offset":1},
  {"orderId":"uuid-5","userId":"user-42","key":null,"partition":1,"offset":7},
  {"orderId":"uuid-6","userId":"user-42","key":null,"partition":2,"offset":1}
]
```

> `key: null` → sticky partitioner distributes across 0, 1, 2.
> Ordering between messages is NOT guaranteed.

### 3. Compare two different userIds

```bash
# user-42 → one partition (always the same)
curl -s -X POST http://localhost:8081/api/orders/demo/keyed \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-42","count":3}' | jq '.[].partition'

# user-99 → different partition (but also always the same)
curl -s -X POST http://localhost:8081/api/orders/demo/keyed \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-99","count":3}' | jq '.[].partition'
```

Result: `user-42` → all `1`, `user-99` → all `0` (or another, but stable).

### 4. Regular order (key = userId)

```bash
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-42","product":"The Pragmatic Programmer","quantity":1,"totalAmount":45.00}' | jq
```

### 5. notification-service logs (key visible)

```bash
docker logs b03-notification-service --tail=30
```

Expected output:

```
╔══════════════════════════════════════╗
║  ORDER CREATED  #1
║  Key: user-42  →  Partition: 1  Offset: 0
║  Order ID  : f47ac10b-...
║  User ID   : user-42
║  Product   : Kafka Book #1 x1
║  Total     : $10.0
║  → Email sent to user user-42
╚══════════════════════════════════════╝
```

### 6. CLI commands inside the container

```bash
# Describe topic in detail (partitions, replication, leader)
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe --topic 03.orders.created

# Consumer group status (offset, lag, partition assignment)
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group notification-service-group
```

Expected output of `--describe --topic 03.orders.created`:

```
Topic: 03.orders.created   PartitionCount: 3   ReplicationFactor: 1
  Topic: 03.orders.created  Partition: 0  Leader: 1  Replicas: 1  Isr: 1
  Topic: 03.orders.created  Partition: 1  Leader: 1  Replicas: 1  Isr: 1
  Topic: 03.orders.created  Partition: 2  Leader: 1  Replicas: 1  Isr: 1
```

### 7. Kafka UI — compare distribution

Open: [http://localhost:8080](http://localhost:8080)

- `Topics` → `03.orders.created` → `Messages`
- The **Key** column shows `user-42` for keyed messages, empty for null-key
- The **Partition** column confirms: same key → same partition

---

## How it works internally

### Producer (order-service) — what changed

#### branch02 (before)

```kotlin
// Key = orderId (UUID) — random, no ordering guarantee
kafkaTemplate.send(topicCreated, event.orderId, event)
kafkaTemplate.send(topicCancelled, orderId, event)
```

#### branch03 (after)

```kotlin
// Key = userId — deterministic, same for all orders from the same user
kafkaTemplate.send(topicCreated, userId, event)
// OrderCancelled also goes to the same partition as OrderCreated for this userId
kafkaTemplate.send(topicCancelled, userId, event)
```

#### New batch methods

```kotlin
fun createBatchKeyed(request: BatchOrderRequest): List<OrderSendResult>
// key = userId → all messages in the same partition
// .get() — synchronous wait → response contains real partition + offset

fun createBatchNoKey(request: BatchOrderRequest): List<OrderSendResult>
// key = null → sticky partitioner distributes across partitions
```

> `kafkaTemplate.send(...).get()` — synchronous wait for broker acknowledgment.
> This is done only for demo endpoints to show real partition/offset values in the response.
> In production, async callbacks are used.

### Consumer (notification-service) — what changed

#### OrderEventListener.kt

```kotlin
// branch02: only partition and offset
log.info("║  Partition: {}  |  Offset: {}", record.partition(), record.offset())

// branch03: key added
log.info("║  Key: {}  →  Partition: {}  Offset: {}",
    record.key(), record.partition(), record.offset())
```

`record.key()` returns the string `"user-42"` or `null` depending on whether a key was set during send.

---

## Project structure (changes relative to branch02)

```
kafka-laboratory/
├── docker-compose-03.yml                   ← ports 8081/8082
├── branch03_keys/
│   ├── order-service/
│   │   └── src/main/kotlin/com/kafkalab/order/
│   │       ├── controller/OrderController.kt  ← new POST /demo/keyed, /demo/round-robin
│   │       ├── model/
│   │       │   ├── BatchOrderRequest.kt        ← NEW: {userId, count, product}
│   │       │   └── OrderSendResult.kt          ← NEW: {orderId, userId, key, partition, offset}
│   │       └── service/OrderService.kt         ← key changed to userId, + createBatchKeyed/NoKey
│   │   └── src/main/resources/
│   │       └── application.yml                ← port 8081
│   └── notification-service/
│       └── src/main/kotlin/com/kafkalab/notification/
│           └── listener/OrderEventListener.kt  ← prints record.key() in logs
│       └── src/main/resources/
│           └── application.yml                ← port 8082
```

---

## What's next — branch04

In the next branch we study **Consumer Groups**:
- Run 3 instances of notification-service in the same consumer group
- Each gets 1 partition from `03.orders.created` (3 partitions / 3 consumers = 1:1)
- Stop one instance → automatic rebalance → 2 consumers handle 1-2 partitions each
- Partition assignment strategies: `RangeAssignor` vs `RoundRobinAssignor`

---
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------

## Presentation slides

**Slide 1: Title**
- Branch 03 — Message Keys
- How Kafka decides which partition to send a message to

---

**Slide 2: The problem with orderId as key**
- In branch02 the key = orderId (UUID) — unique per order
- Different orders from the same user → different partitions
- In multi-consumer scenario: user's events are handled by different instances
- Ordering between orders is not guaranteed → business logic may break

---

**Slide 3: Partition selection algorithm**
- Kafka DefaultPartitioner → Murmur2 hash
- `partition = Math.abs(murmur2(key.getBytes())) % numPartitions`
- Same key → same hash → same partition
- Determinism: independent of time, restarts, send order

---

**Slide 4: userId as the correct key**
- user-42 → hash % 3 = 1 → partition 1 (ALWAYS)
- user-99 → hash % 3 = 0 → partition 0 (ALWAYS)
- All events of the same user → one partition
- In multi-consumer: all events for user-42 → same consumer instance

---

**Slide 5: Ordering guarantee**
- Kafka guarantees ordering ONLY within a single partition
- Between partitions — ordering is not guaranteed
- Conclusion: key = entity identifier → event ordering is guaranteed
- Rule: if event ordering matters → key = entity identifier

---

**Slide 6: null key → StickyPartitioner**
- When key = null → StickyPartitioner (Kafka 2.4+)
- "Sticks" to one partition within a batch
- Then moves to the next partition
- Ordering between messages is NOT guaranteed

---

**Slide 7: Parallelism vs Ordering**
- 3 partitions → up to 3 consumers in parallel
- More partitions → higher throughput
- But: more partitions → weaker ordering guarantees between entities
- Trade-off: choose partition count based on expected load + consumer count

---

**Slide 8: Demo — keyed batch**
- `POST /api/orders/demo/keyed` → userId=user-42, count=6
- Response: all 6 messages in partition=1
- Kafka UI: Key column shows "user-42", Partition = 1

---

**Slide 9: Demo — round-robin batch**
- `POST /api/orders/demo/round-robin` → key=null, count=6
- Response: messages spread across partitions 0, 1, 2 (unevenly)
- Kafka UI: Key empty, Partition varies

---

**Slide 10: record.key() in Consumer**
- `@KafkaListener` → `ConsumerRecord<String, OrderCreatedEvent>`
- `record.key()` → returns string "user-42" or null
- Log: `Key: user-42  →  Partition: 1  Offset: 0`
- Consumer sees the producer's routing decision

---

**Slide 11: Key Takeaways**
- Message key → deterministic partition selection (Murmur2 hash)
- userId > orderId as key: all events for one user → one partition → one consumer instance
- null key → sticky partitioner → even distribution, but no ordering
- Ordering guaranteed ONLY within a partition

---

**Slide 12: What's next — Consumer Groups**
- branch04: Consumer Groups
- 3 notification-service instances → each reads 1 partition
- Stop one → rebalance → partition redistribution
- RangeAssignor vs RoundRobinAssignor

---

## Presentation script

**Slide 1:**
Welcome! Today we look at one of the most important Kafka concepts — message keys.
At first glance it's a simple idea, but it fundamentally determines
how your system handles event ordering.
We'll see why the choice of key can either break or guarantee the correctness of business logic.

**Slide 2:**
In branch02 we used `orderId` as the message key.
OrderId is a UUID, unique for each order.
This means different orders from the same user land in different partitions.
If there's a dependency between orders — for example, a credit limit or saga compensation —
ordering between them is not guaranteed.
This is a classic problem seen in real systems.

**Slide 3:**
How does Kafka decide which partition to send a message to?
Using the Murmur2 algorithm.
Formula: partition = absolute value of the key hash, modulo the number of partitions.
The key property is determinism:
the same string always produces the same hash, and therefore always lands in the same partition.

**Slide 4:**
Let's replace the key from orderId to userId.
Now user-42 always goes to partition 1, user-99 to partition 0.
All events of the same user are collected in one partition.
When in branch04 we run multiple consumer instances,
all events for user-42 will always be handled by the same instance — no race conditions.

**Slide 5:**
It's important to understand: Kafka guarantees ordering ONLY within one partition.
Between different partitions — ordering is not guaranteed.
That's why the choice of key is so important.
General rule: if event ordering matters for an entity —
use that entity's identifier as the key.

**Slide 6:**
What happens if no key is specified?
Kafka 2.4 introduced StickyPartitioner.
It picks one partition and "sticks" to it during a batch send.
This is better than the old round-robin because a larger batch = fewer system calls = better throughput.
But ordering between messages is still not guaranteed.

**Slide 7:**
There's an important trade-off here.
More partitions = more parallelism = higher throughput.
But more partitions also means different events for the same entity may be read by different consumers.
So the recommendation: choose partition count based on expected load
and the future number of consumers in the group.

**Slide 8:**
Let's look at this in practice.
The `/api/orders/demo/keyed` endpoint lets you send N orders with one userId.
The response returns the partition and offset for each message.
As you can see — all 6 messages in partition 1.
Kafka UI confirms: the Key column shows "user-42", Partition = 1 for all.

**Slide 9:**
The second endpoint — `/api/orders/demo/round-robin` — sends the same messages but without a key.
Now partitions vary: 0, 1, 2 in different order.
Kafka UI shows an empty Key column.
Same userId, but without a key — and ordering is no longer guaranteed.

**Slide 10:**
On the consumer side we added key output to logs:
`record.key()` returns the string "user-42" or null.
This is important for debugging — you can always check what routing key the producer used.
Kafka UI also shows the key in the Key column in the Messages section.

**Slide 11:**
Let's summarize.
First: message key determines the partition through Murmur2 hash, and this is deterministic.
Second: userId is better than orderId as a key —
all events for one user in one partition, in one consumer instance.
Third: null key gives sticky partitioner — even distribution, but no ordering guarantees.
And the main rule: ordering is guaranteed ONLY within one partition.

**Slide 12:**
In the next branch, branch04, we move to consumer groups.
We'll run three instances of notification-service and see
how Kafka automatically distributes partitions among consumers.
We'll stop one instance and watch the rebalance —
when the remaining consumers take over the orphaned partitions. See you then!

---

## Test Questions

**Question 1:** What algorithm does Kafka use to determine the partition by key?

A) SHA-256
B) Murmur2
C) MD5
D) CRC32

**Answer:** B — Kafka uses the Murmur2 algorithm to hash the key.
Formula: `partition = Math.abs(murmur2(key.getBytes())) % numPartitions`.

---

**Question 2:** What happens if you send two messages with the same key `userId="user-42"`
to a topic with 3 partitions?

A) They will always land in the same partition
B) They will be distributed between partitions randomly
C) Kafka will reject the second message as a duplicate
D) They will both land in partition 0

**Answer:** A — The same key always produces the same hash → same partition.
This is deterministic and does not depend on the send time.

---

**Question 3:** In which case does Kafka guarantee message processing order?

A) Only if there is one consumer in the group
B) Only within a single partition
C) For all messages in one topic
D) Only if the key is not null

**Answer:** B — Kafka guarantees ordering exclusively within one partition.
Between different partitions — ordering is not guaranteed.

---

**Question 4:** Why is `userId` a better key than `orderId` (UUID)?

A) UUID is longer and takes more space
B) userId is deterministic: all events for one user land in one partition,
   guaranteeing ordering between all their orders
C) orderId is not supported by Kafka
D) UUID cannot be hashed by the Murmur2 algorithm

**Answer:** B — orderId is unique per order, so different orders from the same user
are scattered across different partitions.
With userId, all user events are in one partition → guaranteed ordering between orders.

---

**Question 5:** What is StickyPartitioner and when is it used?

A) An algorithm for hashing keys with special characters
B) A strategy for null-key messages: "sticks" to one partition within a batch
C) A way to bind a partitioner to a specific consumer
D) A method for evenly distributing messages across topics

**Answer:** B — StickyPartitioner picks one partition and sends all messages from one batch to it.
Then moves to the next.
This is more efficient than round-robin, but ordering between batches is not guaranteed.

---

**Question 6:** How many partitions does a consumer group with 1 consumer receive for a topic with 3 partitions?

A) 1
B) 3
C) 0 (consumer cannot read more than one)
D) Depends on the assignor

**Answer:** B — If a consumer group has 1 consumer, it reads all 3 partitions itself.
Kafka assigns each consumer the maximum possible number of partitions.

---

**Question 7:** What does `record.key()` return in a `@KafkaListener` for a message sent without a key?

A) Empty string `""`
B) `null`
C) A UUID generated by Kafka
D) The topic name

**Answer:** B — If the producer didn't specify a key, `record.key()` returns `null`.

---

**Question 8:** What formula determines which partition a keyed message lands in?

A) `ordinal(key) % numPartitions`
B) `hash(key) + numPartitions`
C) `Math.abs(murmur2(key.getBytes())) % numPartitions`
D) `key.hashCode() % numPartitions`

**Answer:** C — Kafka uses Murmur2 hash, takes the absolute value,
and divides by the number of partitions.

---

**Question 9:** What will happen if you increase the number of topic partitions from 3 to 6
after data has already been written?

A) Nothing changes, old messages stay in place
B) New messages with the same key may land in a DIFFERENT partition
C) Kafka automatically redistributes all old messages
D) This is forbidden in Kafka

**Answer:** B — `hash("user-42") % 3 = 1`, but `hash("user-42") % 6 = 4`.
Changing partition count changes routing and breaks ordering guarantees for accumulated data.
That's why partition count should be planned in advance.

---

**Question 10:** For which use cases is null key best suited?

A) When strict processing order is important
B) For even loading of all partitions without ordering requirements
C) When the consumer group has only one instance
D) For exactly-once semantics

**Answer:** B — null key (with StickyPartitioner) works well when ordering doesn't matter
but even load distribution across partitions is needed
(e.g., log messages, metrics).

---

**Question 11:** User `user-42` has two orders. What problem will `orderId` as key cause
in a multi-consumer scenario?

```
OrderCreated (orderId=order-A, userId=user-42) → partition 2
OrderCreated (orderId=order-B, userId=user-42) → partition 0
OrderCancelled (orderId=order-A, userId=user-42) → partition 2
```

A) OrderCreated and OrderCancelled for order-A will land in different partitions
B) Events for different orders of the same user are scattered across partitions —
   in multi-consumer scenario they are handled by different instances without ordering guarantees between them
C) orderId cannot be used as a key in Kafka
D) Increased latency due to random UUID

**Answer:** B — OrderCreated and OrderCancelled for the **same** order (same orderId) —
are in one partition, that's fine.
The problem is different: `order-A` and `order-B` have different UUIDs → different partitions → different consumer instances.
If there's a dependency between orders of the same user, ordering is not guaranteed.
With `userId` as the key, all `user-42` events are always in one partition and on one consumer.

---

**Question 12:** What is routing determinism in Kafka?

A) Kafka always sends new messages to partition 0
B) The same key always produces the same partition, regardless of time and number of attempts
C) Kafka evenly distributes keys across partitions
D) Routing is determined on the consumer side, not the producer

**Answer:** B — Murmur2 is a deterministic function: for the same input
it always produces the same output.
This means `user-42` will always be in the same partition, even after system restart.

---

**Question 13:** Which statement about parallelism and ordering in Kafka is correct?

A) More partitions = better ordering
B) Fewer partitions = higher throughput
C) More partitions = higher throughput, but weaker ordering guarantees between entities
D) Ordering and throughput are unrelated

**Answer:** C — Partitions are the unit of parallelism.
More partitions = more consumers can read in parallel = higher throughput.
But different events for the same entity may be processed by different consumers independently.

---

**Question 14:** What is the difference between `kafkaTemplate.send(...).get()` and regular async `send()`?

A) `.get()` — synchronous wait for result (partition, offset); async — fire and forget
B) `.get()` sends the message twice for reliability
C) Async sends the message to all partitions simultaneously
D) `.get()` is only available for consumer, not producer

**Answer:** A — `.get()` blocks the thread until receiving acknowledgment from the broker.
This allows returning real partition and offset values.
In production, async with callback is typically used to avoid blocking processing.

---

**Question 15:** The producer sends `OrderCreatedEvent` with `key="user-42"`. The topic has 3 partitions.
5 minutes later the producer is restarted. Where will the next message with `key="user-42"` land?

A) In partition 0 (starts from the beginning after restart)
B) In the same partition as before (partition 1)
C) Kafka cannot answer this without context
D) Randomly, as after consumer restart

**Answer:** B — Murmur2 hash is deterministic and does not depend on producer state.
`hash("user-42") % 3` will always give the same result.

---

**Question 16:** Why is the Key column empty in Kafka UI for messages with `key=null`?

A) Kafka UI doesn't support displaying null keys
B) Kafka stores null as an empty string, but UI displays it as an empty field
C) Messages without a key have an empty Key column because the key is absent from metadata
D) This is a bug in Kafka UI version 0.7+

**Answer:** C — If the key is not specified, it is stored as null in message metadata.
Kafka UI displays null as an empty field.

---

**Question 17:** What happens with `RoundRobinAssignor` in a consumer group with 2 consumers
and a topic with 3 partitions?

A) Consumer 1 gets 3 partitions, Consumer 2 — 0
B) Consumer 1 gets partitions 0, 2; Consumer 2 — partition 1
C) Consumer 1 gets partitions 0, 1; Consumer 2 — partition 2
D) Both consumers read all 3 partitions simultaneously

**Answer:** B — RoundRobinAssignor distributes partitions in turn: p0→C1, p1→C2, p2→C1.
Result: C1 gets 0 and 2, C2 — only 1.

---

**Question 18:** Which scenario CORRECTLY demonstrates message key usage?

A) Send all payment events with key=`"payment"` to one topic
B) Send account events with key=`accountId` so all changes to one account are processed in order
C) Send log messages with key=timestamp for chronological order
D) Send all events with key=`null` for even distribution

**Answer:** B — Key = entity identifier (accountId) guarantees
that all changes to one account land in one partition and are processed in the correct order.

---

**Question 19:** A consumer group reads a topic with 3 partitions.
All messages have key=`"fixed-key"`. What problem will arise?

A) Kafka doesn't allow a fixed key
B) All messages will land in one partition → only one consumer will be loaded,
   the rest will be idle
C) Messages will be duplicated across partitions
D) Throughput will increase because hashing is stable

**Answer:** B — If all keys are the same → all messages in one partition →
one partition is read by one consumer.
The rest of the consumers in the group are idle. This is an anti-pattern — hot partition.

---

**Question 20:** For the CCDAK exam: which statement is correct about Kafka Default Partitioner?

A) It uses round-robin for keys and StickyPartitioner for null
B) It uses Murmur2 hash for keys and StickyPartitioner for null keys (Kafka 2.4+)
C) It always uses round-robin regardless of whether a key is present
D) It uses Java `hashCode()` to determine the partition

**Answer:** B — This is a classic CCDAK question.
DefaultPartitioner: if key exists → Murmur2 hash;
if key is null → StickyPartitioner (starting from Kafka 2.4, previously was round-robin).