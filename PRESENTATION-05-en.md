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

# Branch 05
# Multiple Consumer Groups

**Apache Kafka for Certification & Production**

---

## Agenda

1. Why branch04 wasn't enough — one reader for all
2. Multiple Consumer Groups — the core principle
3. Independent offsets in `__consumer_offsets`
4. Fan-out pattern — one producer, N groups
5. Consumer Lag — monitoring group health
6. `auto-offset-reset`: earliest vs latest
7. Replay — resetting offsets to the beginning
8. Demo: stop → publish → restart → catch up
9. Thread-safe accumulators: `AtomicInteger` + `ConcurrentHashMap`
10. Key Takeaways & What's Next

---

## How Can Two Services Get the Same Data?

**Kafka does NOT delete messages after reading**

- Messages are retained until `retention.ms` expires (default 7 days)
- Any number of consumer groups reads the topic independently
- Each group "sees" the topic from its own committed offset
- This is the key difference from RabbitMQ / ActiveMQ

<br>

```
notification-group → offset 15  (reads emails for each order)
analytics-group    → offset 8   (counts statistics, currently lagging)

Same topic — two completely independent read cursors
```

> Kafka is a **log**, not a queue. Messages survive consumer reads.

---

## Multiple Consumer Groups — Architecture

**One producer, two independent consumers:**

```
order-service ──[05.orders.created]──► notification-service-group
                                    └──► analytics-service-group

Each group:
  - has its own committed offset per partition
  - does not know about other groups
  - can lag or replay independently
```

<br>

- `notification-service-group` → sends email notifications
- `analytics-service-group` → counts statistics, builds reports

> Groups are completely isolated. One group's lag does NOT affect the other.

---

## Independent Offsets in `__consumer_offsets`

**Kafka stores offsets per (group, topic, partition):**

```
Key: (group-id, topic-name, partition)  →  committed-offset

notification-service-group, 05.orders.created, 0  →  15
notification-service-group, 05.orders.created, 1  →  12
analytics-service-group,    05.orders.created, 0  →   8
analytics-service-group,    05.orders.created, 1  →   6
```

- notification and analytics offsets are **completely independent**
- Reading by one group has **zero impact** on the other's progress
- `__consumer_offsets` is an internal Kafka topic

> Two groups reading the same topic = two independent bookmarks in the same book.

---

## Consumer Lag

**LAG = LOG-END-OFFSET − COMMITTED-OFFSET**

```
GROUP                    TOPIC             PARTITION  CURRENT  LOG-END  LAG
notification-group       05.orders.created 0          15       15       0   ← real-time
analytics-group          05.orders.created 0           8       15       7   ← lagging
```

<br>

- **LAG = 0** → group is processing in real time
- **LAG > 0** → group is behind (stopped or slow)
- **Growing LAG** → consumer can't keep up with the producer

> Key production metric: **alert when LAG exceeds a threshold**.
> Full monitoring with Grafana dashboards in branch17.

---

## auto-offset-reset: earliest vs latest

**Applied only when a group has NO committed offset (first start or after replay)**

```
auto-offset-reset: earliest
  → read ALL messages from the beginning of the topic
  → use case: analytics, audit, data warehouse

auto-offset-reset: latest
  → read only NEW messages published after group startup
  → use case: real-time UI, live notifications
```

<br>

| Setting | Reads | Use case |
|---------|-------|----------|
| `earliest` | All stored messages | analytics, audit |
| `latest` | Only new messages | real-time services |

> `analytics-service` uses `earliest` — never miss an order.

---

## Fan-out Pattern

**One producer publishes once — unlimited consumers:**

```
order-service
  └── publish to 05.orders.created  (once)

notification-group  → reads independently → sends email
analytics-group     → reads independently → counts stats
audit-group         → reads independently → writes to warehouse
reporting-group     → reads independently → builds dashboard
```

- No data duplication in the topic
- No routing logic required
- Each group independently decides what to do with the data

> Adding a new consumer group requires **zero changes** to the producer.

---

## Demo: Stop → Publish → Restart → Catch Up

**Test the resilience of independent offsets:**

```bash
# Step 1: stop analytics-service
docker stop analytics-service-b05

# Step 2: send 5 orders (notification receives, analytics does NOT)
for i in {1..5}; do
  curl -s -X POST http://localhost:8081/api/orders \
    -H "Content-Type: application/json" \
    -d '{"userId":"user-01","product":"Book","quantity":1,"totalAmount":20.00}'
done

# Step 3: restart analytics
docker start analytics-service-b05
```

**After restart:** analytics reads all 5 missed messages from its saved offset.
Notification was not interrupted at all.

> This is the key advantage of separate offsets: each group is independently resilient.

---

## Replay — Reset Offsets to the Beginning

**Re-process all stored messages from offset 0:**

```bash
# Stop the group first (consumers must be inactive)
docker stop analytics-service-b05

# Reset offsets to earliest
kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group analytics-service-group \
  --topic 05.orders.created \
  --reset-offsets --to-earliest --execute

# Restart — analytics reads ALL stored messages
docker start analytics-service-b05
```

**Use cases:**
- New analytics service on old data
- Bug in business logic — reprocess correctly
- Testing: clean state + replay

> Replay is always possible within `retention.ms` window.

---

## Thread-Safe Accumulators

**Spring Kafka processes multiple partitions in parallel — thread safety matters:**

```kotlin
// WRONG: race condition with multiple threads
var ordersCreated = 0
fun onMessage() { ordersCreated++ }  // ← not atomic!

// CORRECT: AtomicInteger
val ordersCreated = AtomicInteger(0)
fun onMessage() { ordersCreated.incrementAndGet() }

// CORRECT: ConcurrentHashMap for per-key counters
val ordersByUser = ConcurrentHashMap<String, AtomicLong>()
fun onMessage(userId: String) {
    ordersByUser.computeIfAbsent(userId) { AtomicLong(0) }.incrementAndGet()
}
```

> Avoid race conditions — each partition may run in its own thread.

---

## Key Takeaways

- **Multiple Consumer Groups** = independent readers of the same topic
- **`__consumer_offsets`** stores (group, topic, partition) → offset independently
- **Fan-out without copying** — key advantage of Kafka over traditional queues
- **Consumer Lag** = primary metric for production monitoring
- **Replay** is always possible within `retention.ms` window
- **`auto-offset-reset`**: `earliest` for analytics, `latest` for real-time
- **Thread safety** is required when multiple partitions run in parallel

<br>

> Kafka is a log. Every consumer group gets its own independent view.

---

## What's Next — Branch 06

**Offsets & Commits — manual commit for reliability**

- `enable.auto.commit: false` + `ack-mode: MANUAL_IMMEDIATE`
- `ack.acknowledge()` — commit offset only after processing
- `ack.nack(Duration)` — reject with retry after delay
- at-most-once vs **at-least-once** vs exactly-once

<br>

> Auto-commit loses messages on crash.
> Manual commit gives the control back to your code.

---

<!-- _class: title -->

## What's Next — Branch 06
- Manual commit: acknowledge only after success
- nack: reject with controlled retry
- Idempotency requirement
- Delivery guarantee strategies

# Questions?

**Branch 05 — Multiple Consumer Groups**