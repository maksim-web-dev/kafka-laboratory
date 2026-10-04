# Branch 04 — Consumer Groups

---

## `branch04_customer_groups` — what we learn

- Consumer Group — how Kafka distributes partitions among instances of the same service
- Rule: one partition is read by **only one** consumer in a group at a time
- Running 3 instances of `notification-service` in one group — each gets 1 partition
- Rebalance — automatic partition redistribution when a consumer connects or disconnects
- `RangeAssignor` vs `RoundRobinAssignor` — partition assignment strategies
- `ConsumerSeekAware` — tracking partition assignment in Spring Kafka
- Static Group Membership (`group.instance.id`) — minimizing rebalance on restart
- Consumer lag — tracking consumer lag behind producer
- `INSTANCE_ID` env var — identifying a specific instance in logs
- `GET /api/notifications/partitions` — new endpoint for viewing assigned partitions

---

## What changed compared to branch03

- Number of `notification-service` instances: 1 → **3 instances** (notification-service-1/2/3)
- Each instance reads its own partition (1:1 with 3 consumers / 3 partitions)
- Explicitly set `partition.assignment.strategy: RangeAssignor`
- New env var `INSTANCE_ID` — passed to each instance separately
- Logs now include `instanceId`: `Instance: notification-service-2`
- `[REBALANCE]` logs on `ASSIGNED` / `REVOKED` events
- New endpoint `GET /api/notifications/partitions` — shows assigned partitions
- `/api/notifications/count` now returns `instanceId` in the response
- Ports: notification-service-1=8082, notification-service-2=8083, notification-service-3=8084

---

## Architecture

```
POST /api/orders
        │
        ▼
┌───────────────┐   04.orders.created (3 partitions)
│ order-service │──────────────────────────────────┐
│ :8081         │                                  │
└───────────────┘   04.orders.cancelled (1 part.)  │
                ───────────────────────────────────┼─────┐
                                                   │     │
                              ┌────────────────────┼─────┘
                              │                    │
                     partition-0           partition-1    partition-2
                              │                    │           │
                              ▼                    ▼           ▼
                    ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
                    │notification  │  │notification  │  │notification  │
                    │service-1     │  │service-2     │  │service-3     │
                    │:8082         │  │:8083         │  │:8084         │
                    └──────────────┘  └──────────────┘  └──────────────┘
                    └──────────────────────────────────────────────────┘
                                  notification-service-group

                              ┌──────────────────────┐
                              │      Apache Kafka     │
                              │      kafka:9092       │
                              └──────────────────────┘
                                          │
                              ┌──────────────────────┐
                              │      Kafka UI         │
                              │      :8080            │
                              └──────────────────────┘
```

### Topics and their configuration

- **`04.orders.created`** — 3 partitions, retention 7 days —
  new orders, key=userId, read by all 3 instances
- **`04.orders.cancelled`** — 1 partition, retention 7 days —
  cancellations, key=userId, read only by notification-service-1 (RangeAssignor)
- **`04.payments.processed`** — 3 partitions, retention 7 days — reserved for branch05+
- **`04.notifications.sent`** — 1 partition, retention 1 day — reserved for branch05+

> With `RangeAssignor` and unequal partition count — the first consumer gets more.
> `notification-service-1` reads `orders.cancelled[0]` + `orders.created[0]`.

---

## Key concepts of this branch

### Consumer Group and the 1 partition = 1 consumer rule

Kafka guarantees: **one partition is read by exactly one consumer in a group at a time**.
Excess consumers (more than partitions) are idle.

```
3 partitions + 3 consumers → 1:1 (optimal)

notification-service-1 → orders.created[0]
notification-service-2 → orders.created[1]
notification-service-3 → orders.created[2]

3 partitions + 4 consumers → one is idle:
notification-service-4 → (idle, no partition)
```

### RangeAssignor vs RoundRobinAssignor

```
Topics: orders.created (3 partitions), orders.cancelled (1 partition)
Consumers: C1, C2, C3

RangeAssignor (branch04):
  Distributes partitions of each topic separately, left to right:
  C1 → orders.created[0], orders.cancelled[0]   ← takes the remainder
  C2 → orders.created[1]
  C3 → orders.created[2]
  Downside: C1 is more loaded with odd partition counts

RoundRobinAssignor:
  Combines all partitions from all topics, distributes in turn:
  C1 → orders.created[0], orders.created[2]
  C2 → orders.created[1], orders.cancelled[0]
  C3 → (no partitions if only 4 total)
  Better with many topics with varying partition counts
```

### Rebalance

Rebalance — the process of redistributing partitions when the consumer group membership changes.
**Eager rebalance (RangeAssignor):** all consumers stop reading, receive new assignments.

```
Initial state:
[ASSIGNED] notification-service-1 → [orders.cancelled[0], orders.created[0]]
[ASSIGNED] notification-service-2 → [orders.created[1]]
[ASSIGNED] notification-service-3 → [orders.created[2]]

After stopping notification-service-3:
[REVOKED]  notification-service-1 → all partitions revoked
[REVOKED]  notification-service-2 → all partitions revoked
[ASSIGNED] notification-service-1 → [orders.cancelled[0], orders.created[0]]
[ASSIGNED] notification-service-2 → [orders.created[1], orders.created[2]]
```

### Static Group Membership (group.instance.id)

```yaml
spring:
  kafka:
    consumer:
      properties:
        group.instance.id: notification-service-1
```

**Without static membership:**
- Consumer stops → after `session.timeout.ms` (45s) Kafka performs rebalance
- Consumer returns → may get different partitions

**With static membership:**
- Consumer stops → Kafka waits `session.timeout.ms`
- Consumer returns within that time → same partitions, **no rebalance**
- Important for Kubernetes rolling deploy and short restarts

> In branch04 `group.instance.id` is NOT used due to Spring Kafka limitations:
> two `@KafkaListener` in one service → two consumers → same `group.instance.id`
> → `FencedInstanceIdException`. Solution — in branch08.

---

## How to run

```bash
docker compose -f docker-compose-04.yml up --build
```

Check readiness (6 containers):

```bash
docker compose -f docker-compose-04.yml ps
# kafka, kafka-ui, order-service-b04
# notification-service-1-b04, notification-service-2-b04, notification-service-3-b04
```

---

## How to test

### 1. Check assigned partitions for each instance

```bash
curl -s http://localhost:8082/api/notifications/partitions | jq
curl -s http://localhost:8083/api/notifications/partitions | jq
curl -s http://localhost:8084/api/notifications/partitions | jq
```

On Windows:
```cmd
curl -s http://localhost:8082/api/notifications/partitions
curl -s http://localhost:8083/api/notifications/partitions
curl -s http://localhost:8084/api/notifications/partitions
```

Expected responses:

```json
{"instanceId":"notification-service-1","assignedPartitions":["orders.cancelled[0]","orders.created[0]"],"count":2}
{"instanceId":"notification-service-2","assignedPartitions":["orders.created[1]"],"count":1}
{"instanceId":"notification-service-3","assignedPartitions":["orders.created[2]"],"count":1}
```

> `notification-service-1` gets more due to `RangeAssignor`:
> `orders.cancelled` (1 partition) is assigned to the first consumer.

### 2. Send orders and observe distribution

```bash
for user in user-01 user-02 user-03 user-04 user-05 user-06; do
  curl -s -X POST http://localhost:8081/api/orders \
    -H "Content-Type: application/json" \
    -d "{\"userId\":\"$user\",\"product\":\"Book\",\"quantity\":1,\"totalAmount\":20.00}" \
    | jq -r '"Order for \(.userId) → orderId: \(.orderId)"'
done
```

On Windows:
```cmd
curl -s -X POST http://localhost:8081/api/orders ^
  -H "Content-Type: application/json" ^
  -d "{\"userId\":\"user-01\",\"product\":\"Book\",\"quantity\":1,\"totalAmount\":20.00}"
```

Check logs — each instance processed its partition:

```bash
docker logs notification-service-1-b04 --tail=20
docker logs notification-service-2-b04 --tail=20
docker logs notification-service-3-b04 --tail=20
```

Expected log for `notification-service-2`:

```
╔══════════════════════════════════════════╗
║  ORDER CREATED  #2
║  Instance  : notification-service-2
║  Key: user-03  →  Partition: 1  Offset: 2
║  Order ID  : ...
║  User ID   : user-03
╚══════════════════════════════════════════╝
```

### 3. Demonstrate rebalance — stop one instance

```bash
docker stop notification-service-3-b04
```

Wait ~10–15 seconds. Check redistribution:

```bash
curl -s http://localhost:8082/api/notifications/partitions | jq
curl -s http://localhost:8083/api/notifications/partitions | jq
```

Expected result after rebalance:

```json
{"instanceId":"notification-service-1","assignedPartitions":["orders.cancelled[0]","orders.created[0]"],"count":2}
{"instanceId":"notification-service-2","assignedPartitions":["orders.created[1]","orders.created[2]"],"count":2}
```

`notification-service-2` now reads 2 partitions instead of 1.

### 4. Recovery — start it back

```bash
docker start notification-service-3-b04
```

Logs will show rebalance and return to even distribution.

### 5. CLI consumer group monitoring

```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group notification-service-group
```

Expected output (3 active members):

```
GROUP                       TOPIC              PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
notification-service-group  04.orders.created  0          6               6               0
notification-service-group  04.orders.created  1          4               4               0
notification-service-group  04.orders.created  2          5               5               0
```

### 6. Kafka UI

Open: [http://localhost:8080](http://localhost:8080)

- `Consumer Groups` → `notification-service-group` → **Members** tab
- Shows 3 members, each with assigned partitions
- After stopping one → 2 members, one reads 2 partitions

---

## How it works internally

### application.yml (notification-service) — what changed

```yaml
# branch03 (before):
spring:
  kafka:
    consumer:
      group-id: notification-service-group
      # assignment strategy not set (default)

# branch04 (after):
spring:
  kafka:
    consumer:
      group-id: notification-service-group
      properties:
        partition.assignment.strategy: org.apache.kafka.clients.consumer.RangeAssignor

instance:
  id: ${INSTANCE_ID:notification-service-1}
```

### OrderEventListener.kt — what changed

```kotlin
// branch03: just @KafkaListener
class OrderEventListener { ... }

// branch04: implements ConsumerSeekAware for tracking rebalance
class OrderEventListener(
    @Value("\${instance.id}") private val instanceId: String
) : ConsumerSeekAware {

    override fun onPartitionsAssigned(assignments, callback) {
        log.info("[ASSIGNED] Instance [{}] ASSIGNED: {}", instanceId,
            assignments.keys.map { "${it.topic()}[${it.partition()}]" })
    }

    override fun onPartitionsRevoked(partitions) {
        log.info("[REVOKED] Instance [{}] REVOKED: {}", instanceId,
            partitions.map { "${it.topic()}[${it.partition()}]" })
    }
}
```

`ConsumerSeekAware` — Spring Kafka interface.
Methods `onPartitionsAssigned` / `onPartitionsRevoked` are called automatically on each rebalance.

### NotificationController.kt — new endpoint

```kotlin
@GetMapping("/partitions")
fun assignedPartitions(): Map<String, Any> {
    val parts = listener.getAssignedPartitions()
        .map { "${it.topic()}[${it.partition()}]" }.sorted()
    return mapOf(
        "instanceId" to instanceId,
        "assignedPartitions" to parts,
        "count" to parts.size
    )
}
```

### docker-compose-04.yml — three instances of the same image

```yaml
notification-service-1:
  build: branch04_customer_groups/notification-service
  container_name: notification-service-1-b04
  ports:
    - "8082:8082"
  environment:
    - INSTANCE_ID=notification-service-1

notification-service-2:
  build: branch04_customer_groups/notification-service
  container_name: notification-service-2-b04
  ports:
    - "8083:8082"         # external 8083 → internal 8082
  environment:
    - INSTANCE_ID=notification-service-2

notification-service-3:
  build: branch04_customer_groups/notification-service
  container_name: notification-service-3-b04
  ports:
    - "8084:8082"         # external 8084 → internal 8082
  environment:
    - INSTANCE_ID=notification-service-3
```

All three build **the same Docker image** — `INSTANCE_ID` is passed via env var.

---

## Project structure (changes relative to branch03)

```
kafka-laboratory/
├── docker-compose-04.yml                      ← 3x notification-service, ports 8082/8083/8084
├── branch04_customer_groups/
│   ├── notification-service/
│   │   └── src/main/kotlin/com/kafkalab/notification/
│   │       ├── controller/NotificationController.kt  ← NEW: instanceId + GET /partitions
│   │       └── listener/OrderEventListener.kt        ← ConsumerSeekAware + instanceId in logs
│   │   └── src/main/resources/
│   │       └── application.yml                       ← RangeAssignor, INSTANCE_ID
│   └── order-service/
│       └── src/main/kotlin/com/kafkalab/order/
│           └── config/KafkaTopicConfig.kt            ← topics 04.orders.*
```

---

## What's next — branch05

In the next branch we study **Multiple Consumer Groups**:
- Two independent services read **the same** topic
- `notification-service-group` and `analytics-service-group` — each with its own offset
- Stop `analytics-service`, publish 10 events → analytics catches up on missed events at startup
- Demonstrating "replay": new consumer group reads topic from the beginning

---
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------

## Presentation slides

**Slide 1: Title**
- Branch 04 — Consumer Groups
- How Kafka distributes partitions among service instances

---

**Slide 2: The branch03 problem**
- 1 notification-service reads all 3 partitions
- Throughput limited by one consumer's speed
- On failure — all reading stops
- Solution: run multiple instances — Consumer Group

---

**Slide 3: Consumer Group rule**
- One partition → only one consumer in a group at a time
- 3 partitions + 3 consumers → each reads 1 partition (1:1)
- 3 partitions + 4 consumers → one is idle (excess)
- 3 partitions + 2 consumers → one reads 2 partitions

---

**Slide 4: RangeAssignor**
- Sorts partitions of each topic separately
- Distributes left to right across consumers
- C1 → orders.created[0] + orders.cancelled[0]
- C2 → orders.created[1], C3 → orders.created[2]
- Downside: first consumer takes the remainder (uneven)

---

**Slide 5: Rebalance — what it is**
- Rebalance = partition redistribution when group membership changes
- Triggers: new consumer joined / consumer left (crash or graceful)
- Eager rebalance: all stop → receive new assignments → resume reading
- Stop-the-world pause → Consumer lag increases

---

**Slide 6: Rebalance — demo**
- Stop notification-service-3
- Kafka waits session.timeout.ms (~45s)
- notification-service-2 receives partition-2 in addition
- Start again → another rebalance → even distribution

---

**Slide 7: Static Group Membership**
- group.instance.id = notification-service-1
- Consumer returns before session.timeout → same partitions without rebalance
- Important for Kubernetes rolling deploy
- NOT used in branch04 due to FencedInstanceIdException (2 listeners)
- Solution: branch08

---

**Slide 8: ConsumerSeekAware**
- Spring Kafka interface for tracking partition assignment
- onPartitionsAssigned() → log [ASSIGNED]
- onPartitionsRevoked() → log [REVOKED]
- Allows reacting to rebalance: save state, flush buffer

---

**Slide 9: INSTANCE_ID env var**
- One Docker image → three instances with different INSTANCE_ID
- notification-service-1/2/3 passed via environment
- application.yml: instance.id = ${INSTANCE_ID:notification-service-1}
- GET /api/notifications/partitions → shows instanceId + partition list

---

**Slide 10: Consumer Lag monitoring**
- kafka-consumer-groups --describe --group notification-service-group
- Columns: PARTITION, CURRENT-OFFSET, LOG-END-OFFSET, LAG, CONSUMER-ID
- LAG = LOG-END-OFFSET - CURRENT-OFFSET
- LAG > 0 → consumer is lagging → monitoring needed (branch17)

---

**Slide 11: Key Takeaways**
- Consumer Group = horizontal read scaling
- 1 partition = 1 consumer (never more in one group)
- Rebalance is inevitable when group membership changes, but can be minimized
- RangeAssignor — default in Spring Kafka, uneven with different topics
- Static Membership — less rebalance for stable consumers
- Consumer lag — key performance metric

---

**Slide 12: What's next — Multiple Consumer Groups**
- branch05: two independent services read one topic
- notification-group and analytics-group — separate offsets
- Replay: new group reads from earliest
- Independent scaling of each group

---

## Presentation script

**Slide 1:**
Welcome! In this branch we solve a real scaling problem.
In branch03 we had one notification-service reading all three partitions.
This means the total throughput is limited by one process's speed.
Today we'll run three instances and see how Kafka distributes work between them.

**Slide 2:**
The branch03 problem is simple: one consumer — one processing thread.
If we have 3 partitions but 1 consumer — we're not using Kafka's parallelism.
Consumer Group is a mechanism that allows multiple instances of the same service
to read different partitions simultaneously.

**Slide 3:**
The main rule of a consumer group: one partition is read by only one consumer in the group.
This is an iron guarantee from Kafka.
We have 3 partitions and 3 consumers — ideal 1:1 distribution.
If consumers exceed partitions — excess consumers simply wait.
If fewer — one consumer reads multiple partitions.

**Slide 4:**
RangeAssignor strategy — the default in Spring Kafka.
It takes each topic separately, sorts partitions and distributes left to right.
But there's a nuance: if partition count doesn't divide evenly by consumer count —
the first consumer always takes the remainder.
In our case notification-service-1 reads both orders.cancelled and orders.created[0].

**Slide 5:**
Rebalance is the process of partition redistribution when group membership changes.
It happens when a new consumer joins or an existing one disconnects.
Eager rebalance, used by RangeAssignor: all consumers first stop reading,
then Kafka redistributes partitions and all start again.
This is a "stop the world" pause — during it consumer lag may grow.

**Slide 6:**
Let's see rebalance in practice.
Stop notification-service-3 with docker stop.
Kafka waits session.timeout.ms — 45 seconds by default.
After that notification-service-2 receives partition-2 in addition to its partition-1.
Start the service back — another rebalance occurs and distribution returns to 1:1.

**Slide 7:**
Static Group Membership is an optimization to reduce rebalance frequency.
If group.instance.id is set, Kafka "remembers" a specific consumer by this identifier.
If the consumer restarts and returns before session.timeout —
it receives the same partitions without a full rebalance.
This is especially important for Kubernetes where pods often restart during rolling deploy.

**Slide 8:**
ConsumerSeekAware is a Spring Kafka interface for tracking partition assignment changes.
We implement two methods: onPartitionsAssigned is called when a consumer gets new partitions,
onPartitionsRevoked — when partitions are revoked before rebalance.
This allows logging rebalance events and reacting to them:
for example, saving current state or flushing a buffer before losing a partition.

**Slide 9:**
To identify each instance we use the INSTANCE_ID env var.
One Docker image runs three times with different values: notification-service-1, 2, 3.
application.yml reads this value via a Spring placeholder.
The new endpoint GET /api/notifications/partitions shows
which partitions are currently assigned to a specific instance — convenient for debugging.

**Slide 10:**
Consumer lag — the key metric for monitoring consumer groups.
The kafka-consumer-groups --describe command shows for each partition:
the consumer's current offset, the last offset in the partition, and the difference — lag.
If lag keeps growing — the consumer can't keep up with the producer.
In branch17 we'll connect Grafana and monitor lag in real time.

**Slide 11:**
Let's summarize. Consumer Group is horizontal read scaling in Kafka.
Main rule: one partition is read by only one consumer in the group.
Rebalance is inevitable when group membership changes, but static membership can minimize it.
RangeAssignor — default, but not always even.
Consumer lag — first signal that the system can't handle the load.

**Slide 12:**
In the next branch, branch05, we see the other side of consumer groups.
Two different services will read the same topic independently.
notification-service-group and analytics-service-group — each with its own offset.
Important property: stop one service, accumulate messages,
start again — it catches up on everything missed. See you then!

---

## Test Questions

**Question 1:** How many consumers in a group can simultaneously read one partition?

A) Unlimited
B) Exactly one
C) Depends on the number of partition replicas
D) Two, if both have the same group.instance.id

**Answer:** B — Kafka's core guarantee: one partition is read by exactly one consumer
in a group at a time. This is a fundamental property of Consumer Group.

---

**Question 2:** A group has 3 consumers and a topic with 3 partitions.
What happens if a fourth consumer is added?

A) The fourth consumer reads all 3 partitions in parallel
B) The fourth consumer will be idle — there's no partition for it
C) Kafka automatically increases partition count to 4
D) An error occurs — you cannot have more consumers than partitions

**Answer:** B — Kafka cannot assign a partition to a consumer if all are already taken.
The fourth consumer simply waits until one of the three disconnects.

---

**Question 3:** What is Rebalance in Kafka?

A) Redistribution of messages between topics
B) The process of redistributing partitions between consumers in a group when its membership changes
C) Automatic increase of partition count under growing load
D) Synchronization of offsets between different consumer groups

**Answer:** B — Rebalance happens when a consumer joins the group or leaves it.
Kafka redistributes partitions among active members.

---

**Question 4:** How does RangeAssignor differ from RoundRobinAssignor?

A) RangeAssignor is faster, RoundRobinAssignor is more accurate
B) RangeAssignor distributes each topic's partitions separately;
   RoundRobinAssignor combines all partitions from all topics and distributes in turn
C) RoundRobinAssignor is only available in Confluent Kafka
D) RangeAssignor is used only for a single topic

**Answer:** B — The key difference is in approach.
RangeAssignor may give the first consumer more with odd partition counts.
RoundRobinAssignor is more even with multiple topics.

---

**Question 5:** What is Consumer Lag?

A) Delay between sending a message and receiving it by the producer
B) Difference between the last offset in a partition and the consumer's current offset
C) Time waiting for a new message when the partition is empty
D) Number of messages in the producer's buffer

**Answer:** B — Consumer Lag = `LOG-END-OFFSET - CURRENT-OFFSET`.
Shows how many messages the consumer hasn't read yet. Growing lag — a signal of problems.

---

**Question 6:** Why is `group.instance.id` (Static Group Membership) needed?

A) To assign a fixed partition to a specific consumer forever
B) So a consumer after a short restart gets back the same partitions without full rebalance
C) To encrypt traffic between consumer and broker
D) To identify a consumer in Kafka logs

**Answer:** B — Static Membership allows Kafka to "remember" a consumer by its instance ID.
If it returns before `session.timeout.ms` — it gets the same partitions, no rebalance occurs.

---

**Question 7:** Which command shows the current state of a consumer group (offsets, lag, members)?

A) `kafka-topics --describe --group notification-service-group`
B) `kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group notification-service-group`
C) `kafka-configs --describe --entity-type groups`
D) `kafka-log-dirs --bootstrap-server localhost:9092 --group notification-service-group`

**Answer:** B — `kafka-consumer-groups --describe` shows for each partition:
CURRENT-OFFSET, LOG-END-OFFSET, LAG and the consumer ID that reads it.

---

**Question 8:** Three partitions, two consumers with `RangeAssignor`. How are partitions distributed?

A) C1 → p0, p1; C2 → p2
B) C1 → p0; C2 → p1, p2
C) C1 → p0, p2; C2 → p1
D) C1 → p0, p1, p2; C2 → idle

**Answer:** A — RangeAssignor sorts partitions and divides: 3 partitions / 2 consumers = 1.5,
so the first gets ceil(1.5)=2. Result: C1→[p0, p1], C2→[p2].

---

**Question 9:** What does "Eager Rebalance" mean in Kafka?

A) Rebalance happens immediately without waiting for consumer acknowledgment
B) All consumers first revoke their partitions, then receive new assignments
C) Only "extra" partitions move between consumers without stopping
D) Rebalance only happens when adding a new consumer, not when removing

**Answer:** B — Eager rebalance (RangeAssignor, RoundRobinAssignor): first REVOKED for all,
then ASSIGNED. This is "stop the world" — during this all consumers pause processing.

---

**Question 10:** A consumer in a group received no partition. What is the reason?

A) Consumer didn't connect to the correct bootstrap-server
B) There are more consumers in the group than partitions in the topic
C) group-id is not specified in configuration
D) Consumer uses the wrong deserializer

**Answer:** B — If there are more consumers than partitions, excess ones have nothing to read.
They remain in the group and become active at the next rebalance
(for example, if someone disconnects).