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

# Branch 08
# Consumer Configuration

**Apache Kafka for Certification & Production**

---

## Agenda

1. EAGER rebalance — the stop-the-world problem
2. COOPERATIVE (Incremental) rebalance
3. `CooperativeStickyAssignor`
4. Static Group Membership — `group.instance.id`
5. `max.poll.records` — protection against timeout
6. `session.timeout.ms` vs `heartbeat.interval.ms`
7. `max.poll.interval.ms` — the hidden timeout
8. Graceful shutdown
9. Demo: COOPERATIVE rebalance in action
10. Key Takeaways & What's Next

---

## EAGER Rebalance — Stop the World

**Any group membership change stops ALL consumers:**

```
EAGER rebalance sequence:
  Step 1: consumer-3 joins the group
  Step 2: ALL consumers REVOKE all their partitions
            consumer-1: REVOKED [p0, p1]
            consumer-2: REVOKED [p2]
  Step 3: Kafka recalculates all assignments
  Step 4: ALL consumers receive new ASSIGNED events
            consumer-1: ASSIGNED [p0]
            consumer-2: ASSIGNED [p1]
            consumer-3: ASSIGNED [p2]
  Step 5: reading resumes
```

- **Downtime** = seconds to a minute during rebalance
- `RangeAssignor` and `RoundRobinAssignor` are EAGER by design
- Consumer Lag grows during the pause

> Every deployment causes two EAGER rebalances: disconnect + reconnect.

---

## COOPERATIVE Rebalance

**Only "migrating" partitions are temporarily paused:**

```
COOPERATIVE rebalance — consumer-3 joins:
  Round 1: Identify which partitions need to move
    consumer-1 REVOKES [p1]   ← only p1 migrates
    consumer-2 keeps  [p2]    ← no interruption
    consumer-1 keeps  [p0]    ← no interruption
  Round 2: Assign released partitions
    consumer-3 ASSIGNED [p1]

Non-migrating partitions continue reading WITHOUT interruption
```

<br>

- Introduced in Kafka 2.4, production standard in Kafka 3.x
- **`CooperativeStickyAssignor`** — also minimizes partition movement

> COOPERATIVE = incremental rebalance with minimal downtime impact.

---

## Static Group Membership

**`group.instance.id` — identify consumer across restarts**

```
Without static membership (dynamic):
  service restart → Kafka sees: disconnect + reconnect
  → 2 rebalances per deployment
  session.timeout.ms delay between them

With static membership:
  group.instance.id: notification-service-1
  service restarts → Kafka recognizes same instance-id
  → waits session.timeout.ms before declaring dead
  → if returns within timeout → 0 rebalances, same partitions ✓
```

<br>

**Critical for:**
- Kubernetes rolling deployments (pods restart frequently)
- Short maintenance windows

> Static membership + COOPERATIVE = near-zero downtime on deploy.

---

## max.poll.records

**Limits the batch size per `poll()` call:**

```
poll() → returns up to max.poll.records messages

Problem without tuning:
  max.poll.records=500 (default)
  Each message takes 200ms to process (DB call)
  500 × 200ms = 100 seconds per batch
  max.poll.interval.ms=300s → borderline dangerous

Solution:
  max.poll.records=10
  10 × 200ms = 2 seconds → safely within interval
```

<br>

- Typical production values: **10–500** (depends on processing time)
- Smaller batch → shorter processing time → stays within poll interval

> Common mistake: slow DB calls + large `max.poll.records` = mystery rebalance.

---

## session.timeout.ms vs heartbeat.interval.ms

**Two separate timeout mechanisms:**

```
heartbeat.interval.ms (default 3s):
  Background thread sends heartbeat to broker
  Signals: "consumer is alive, just processing"
  Does NOT require poll() to be called

session.timeout.ms (default 45s):
  If no heartbeat received → broker declares consumer dead
  Triggers rebalance

Rule of three:
  heartbeat.interval.ms < session.timeout.ms / 3
  Example: heartbeat=3s, session.timeout=45s → safe
```

<br>

- **Smaller `session.timeout`** → faster failure detection, more false positives
- **Larger `session.timeout`** → slower failure detection, fewer false positives

---

## max.poll.interval.ms

**Tracks `poll()` calls, not heartbeats:**

```
max.poll.interval.ms (default 300s = 5 min):
  If time between consecutive poll() calls exceeds this → broker kick-out

Common mistake:
  Slow synchronous operation between polls:
    poll() → fetch 100 messages → call slow DB → 6 minutes → next poll()
    → broker kicks consumer out → REBALANCE

Solutions:
  Option 1: reduce max.poll.records (fewer messages per poll)
  Option 2: increase max.poll.interval.ms (if slow processing is expected)
  Option 3: move heavy work to async (non-blocking)
```

> heartbeat thread can still send heartbeats while poll() is delayed —
> session.timeout alone won't save you here.

---

## Graceful Shutdown

**Zero message loss on deployment:**

```
SIGTERM received:
  Step 1: Spring stops accepting new HTTP requests
  Step 2: Kafka consumer finishes current batch
  Step 3: commitSync() — commit offset before shutdown
  Step 4: COOPERATIVE REVOKE — release partitions cleanly
  Step 5: other consumers receive ASSIGNED for released partitions
  Step 6: process exits

Without graceful shutdown:
  Uncommitted offsets → messages reprocessed after restart
  Sudden EAGER rebalance → all consumers pause
```

> Graceful shutdown = the last line of defense for message delivery guarantees.

---

## Demo — COOPERATIVE in Action

**Start 2 instances, stop one, observe logs:**

```bash
# Start: notification-1 → [p0, p1], notification-2 → [p2]
docker compose -f docker-compose-08.yml up --build

# Stop notification-2
docker stop notification-service-2-b08

# Watch logs: COOPERATIVE behavior
docker logs notification-service-1-b08 --tail=20
```

**Expected logs:**
```
notification-2: [REVOKED] orders.created[2]         ← only p2 released
notification-1: [ASSIGNED] orders.created[2]        ← gets p2
notification-1: [KEPT]    orders.created[0], [1]    ← never interrupted
```

> notification-1 continues reading p0 and p1 WITHOUT any pause.

---

## Key Takeaways

- **`CooperativeStickyAssignor`** = production standard in Kafka 3.x
- **Static Membership** reduces rebalances on rolling restart (Kubernetes)
- **`max.poll.records`** protects against `max.poll.interval.ms` timeout
- **Rule of three:** `heartbeat.interval < session.timeout / 3`
- **`max.poll.interval`** tracks `poll()` calls, not heartbeats — a hidden timeout
- **Graceful shutdown** = zero lost messages on deploy
- **COOPERATIVE + Static** = near-zero downtime during rolling deployments

<br>

> Rebalance is not just a technical detail — it's a direct cause of production downtime.

---

## What's Next — Branch 09

**Error Handling & Dead Letter Topic**

- `@RetryableTopic` — non-blocking retry through Kafka topics
- Automatic retry topic creation with exponential backoff
- `@DltHandler` — manual intervention after exhausted retries
- Retry headers: `kafka_original_topic`, `kafka_exception-message`

---

<!-- _class: title -->

## What's Next — Branch 09
- Non-blocking retry via Kafka topics
- Dead Letter Topic after N failures
- @DltHandler: alert, log, manual review
- Retry state survives service restart

# Questions?

**Branch 08 — Consumer Configuration**