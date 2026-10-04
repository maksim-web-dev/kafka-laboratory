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

# Branch 04
# Consumer Groups

**Apache Kafka for Certification & Production**

---

## Agenda

1. Why branch03 wasn't enough
2. Consumer Group — the fundamental rule
3. Partition assignment strategies: RangeAssignor vs RoundRobinAssignor
4. Rebalance — what, when, and how
5. Static Group Membership
6. `ConsumerSeekAware` — tracking rebalance events
7. `INSTANCE_ID` — one image, many instances
8. Consumer Lag — monitoring group health
9. Key Takeaways & What's Next

---

## Branch03 — The Problem

**One notification-service reads all 3 partitions:**

```
1 notification-service → partition 0 + partition 1 + partition 2
Max throughput = speed of a single consumer
```

- All processing bottlenecked by one process
- Single point of failure — if it crashes, **all reading stops**
- No parallel processing despite having 3 partitions

<br>

> **Solution:** run multiple instances in one **Consumer Group**

---

## Consumer Group — The Fundamental Rule

**One partition is read by exactly one consumer in the group at a time**

```
3 partitions + 3 consumers → 1:1 (optimal)
  notification-service-1 → orders.created[0]
  notification-service-2 → orders.created[1]
  notification-service-3 → orders.created[2]

3 partitions + 4 consumers → one sits idle:
  notification-service-4 → (idle, no partition assigned)

3 partitions + 2 consumers → one reads two partitions:
  notification-service-1 → orders.created[0] + orders.created[1]
  notification-service-2 → orders.created[2]
```

> **Rule:** you cannot have more useful consumers than partitions.
> Extra consumers wait idle.

---

## RangeAssignor — How It Works

**Default strategy in Spring Kafka**

```
Topics: orders.created (3 partitions), orders.cancelled (1 partition)
Consumers: C1, C2, C3

RangeAssignor — assigns each topic's partitions independently:
  C1 → orders.created[0], orders.cancelled[0]   ← gets the remainder
  C2 → orders.created[1]
  C3 → orders.created[2]
```

<br>

**RoundRobinAssignor — merges all partitions, distributes in turn:**

```
  C1 → orders.created[0], orders.created[2]
  C2 → orders.created[1], orders.cancelled[0]
  C3 → (idle if only 4 partitions total)
```

> **Downside of RangeAssignor:** C1 always gets the "overflow"
> when partition count doesn't divide evenly.

---

## Rebalance — What It Is

**Rebalance = redistribution of partitions when group membership changes**

**Triggers:**
- New consumer joins the group
- Consumer disconnects (crash or graceful shutdown)

<br>

**Eager Rebalance (RangeAssignor behavior):**

```
Step 1: ALL consumers stop reading → [REVOKED] for everyone
Step 2: Kafka recalculates assignments
Step 3: All consumers receive new assignments → [ASSIGNED]
Step 4: Reading resumes
```

> "Stop the world" pause — Consumer Lag grows during rebalance.

---

## Rebalance — Demo

**Stop `notification-service-3`:**

```bash
docker stop notification-service-3-b04
```

Kafka waits `session.timeout.ms` (~45 seconds), then:

```
Before:
  service-1 → orders.cancelled[0], orders.created[0]
  service-2 → orders.created[1]
  service-3 → orders.created[2]

After rebalance:
  service-1 → orders.cancelled[0], orders.created[0]
  service-2 → orders.created[1], orders.created[2]   ← now reads 2 partitions
```

**Restart → another rebalance → back to 1:1 distribution**

---

## Static Group Membership

**`group.instance.id` — minimize rebalance on restart**

```yaml
spring:
  kafka:
    consumer:
      properties:
        group.instance.id: notification-service-1
```

<br>

**Without static membership:**
- Consumer stops → after `session.timeout.ms` Kafka triggers rebalance
- Consumer returns → may receive **different** partitions

**With static membership:**
- Consumer stops → Kafka waits `session.timeout.ms`
- Consumer returns within that window → **same partitions, no rebalance**

> Critical for Kubernetes rolling deploys and short restarts.
> Branch04 does NOT use it — `FencedInstanceIdException` with 2 listeners. Fixed in branch08.

---

## ConsumerSeekAware — Tracking Rebalance

**Spring Kafka interface for partition assignment events**

```kotlin
class OrderEventListener(
    @Value("\${instance.id}") private val instanceId: String
) : ConsumerSeekAware {

    override fun onPartitionsAssigned(assignments, callback) {
        log.info("[ASSIGNED] Instance [{}]: {}", instanceId,
            assignments.keys.map { "${it.topic()}[${it.partition()}]" })
    }

    override fun onPartitionsRevoked(partitions) {
        log.info("[REVOKED] Instance [{}]: {}", instanceId,
            partitions.map { "${it.topic()}[${it.partition()}]" })
    }
}
```

> Use `onPartitionsRevoked` to flush buffers or save state
> before losing partition ownership.

---

## INSTANCE_ID — One Image, Many Instances

**One Docker image runs as three separate instances:**

```yaml
notification-service-1:
  build: branch04_customer_groups/notification-service
  ports: ["8082:8082"]
  environment:
    - INSTANCE_ID=notification-service-1

notification-service-2:
  ports: ["8083:8082"]      # external 8083 → internal 8082
  environment:
    - INSTANCE_ID=notification-service-2

notification-service-3:
  ports: ["8084:8082"]      # external 8084 → internal 8082
  environment:
    - INSTANCE_ID=notification-service-3
```

**New endpoint:** `GET /api/notifications/partitions`

```json
{"instanceId":"notification-service-2","assignedPartitions":["orders.created[1]"],"count":1}
```

---

## Consumer Lag — Monitoring Group Health

**`kafka-consumer-groups --describe` — the essential monitoring command:**

```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group notification-service-group
```

```
GROUP                      TOPIC             PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
notification-service-group 04.orders.created 0          6               6               0
notification-service-group 04.orders.created 1          4               4               0
notification-service-group 04.orders.created 2          5               5               0
```

> **Consumer Lag** = `LOG-END-OFFSET − CURRENT-OFFSET`
> Growing lag → consumer can't keep up → alert!
> Full observability in branch17 with Grafana dashboards.

---

## Key Takeaways

- **Consumer Group** = horizontal scaling of Kafka reads
- **1 partition = 1 consumer** in a group — this is an iron guarantee
- **Rebalance** is inevitable when group membership changes — minimize with Static Membership
- **RangeAssignor** is the Spring Kafka default — uneven distribution with mismatched partition counts
- **RoundRobinAssignor** — more balanced when multiple topics have different partition counts
- **`ConsumerSeekAware`** — hook into ASSIGNED/REVOKED events to react to rebalance
- **Consumer Lag** = the primary health metric for consumer groups
- **`INSTANCE_ID`** env var — one image, many configurable instances

<br>

> Consumer Groups are Kafka's horizontal scaling primitive.
> Design partition counts before deployment — they're hard to change later.

---

## What's Next — Branch 05

**Multiple Consumer Groups**

- Two independent services read the **same** topic
- `notification-service-group` and `analytics-service-group` — each with their own offset
- Stop `analytics-service`, publish 10 events → analytics catches up on restart
- **Replay:** new consumer group reads the topic from the beginning (`earliest`)
- Independent scaling of each group

<br>

> Each consumer group gets its own independent view of every topic.

---

<!-- _class: title -->

## What's Next — Branch 05
- Two services, one topic — independent offsets
- Stop one service, publish events, restart — it catches up
- Replay from earliest offset
- Scale each group independently

# Questions?

**Branch 04 — Consumer Groups**