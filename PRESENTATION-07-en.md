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

# Branch 07
# Producer Configuration

**Apache Kafka for Certification & Production**

---

## Agenda

1. `acks=0`, `acks=1`, `acks=all` — three reliability levels
2. ISR — In-Sync Replicas
3. `min.insync.replicas` and `NotEnoughReplicasException`
4. Idempotent Producer — sequence numbers
5. `linger.ms` + `batch.size` — batching for throughput
6. `delivery.timeout.ms` + retries
7. Three configurations for different use-cases
8. `BenchmarkService` — measuring the difference
9. Key Takeaways & What's Next

---

## acks — Three Confirmation Levels

**`acks` controls how many brokers must confirm before producer considers send successful:**

```
acks=0  (fire and forget):
  Producer sends → does NOT wait for any response
  Fastest possible, but loss on broker crash → at-most-once

acks=1  (leader ack):
  Producer sends → waits for leader broker confirmation
  Message may be lost if leader crashes before replication → at-most-once

acks=all  (quorum ack):
  Producer sends → waits for ALL ISR brokers to confirm
  Safest — no loss as long as min.insync.replicas are alive → at-least-once
```

> With a single broker: difference between `acks=1` and `acks=all` is minimal.
> In production with 3 brokers: `acks=all` is the only safe choice.

---

## ISR and min.insync.replicas

**ISR = In-Sync Replicas: brokers synchronized with the leader**

```
Cluster: 3 brokers, replication-factor=3
Leader: broker-1
ISR: [broker-1, broker-2, broker-3]

If broker-3 lags behind (replica.lag.time.max.ms exceeded):
ISR: [broker-1, broker-2]  ← broker-3 removed from ISR

min.insync.replicas=2:
  acks=all → needs at least 2 ISR to confirm
  If only 1 ISR available → NotEnoughReplicasException
```

<br>

**Production gold standard:** 3 brokers, RF=3, `min.insync.replicas=2`

> Tolerates one broker failure while maintaining full write guarantees.

---

## Idempotent Producer

**Problem: timeout + retry = duplicate in Kafka**

```
Without idempotence:
  producer.send() → network timeout → retry
  Broker already received first attempt → writes TWICE
  Payment charged twice → critical error

With enable.idempotence=true:
  Kafka assigns producer-id at startup
  Each message gets sequence-number per partition
  On retry: broker detects same producer-id + sequence → silently skips
  Result: exactly ONE write per message ✓

Requirements:
  acks=all
  retries > 0
  max.in.flight.requests.per.connection ≤ 5
```

> Idempotent producer is automatically enabled with `acks=all` in Kafka 3.0+.

---

## linger.ms + batch.size

**Batching: accumulate multiple messages before sending**

```
linger.ms=0 (default):
  Every message → separate network request
  Lowest latency, smallest batch, most overhead

linger.ms=20:
  Producer waits up to 20ms to accumulate messages
  Sends when: batch.size reached OR linger.ms elapsed
  Higher throughput, lower per-message overhead
```

<br>

```
batch.size=16384 (16KB default):
  Maximum bytes in one batch per partition
  When full: send immediately regardless of linger.ms

batch.size=65536 (64KB):
  Larger batches → better compression → higher throughput
```

> `linger.ms` and `batch.size`: trade latency for throughput.

---

## delivery.timeout.ms

**Total time budget from `send()` to success or final failure:**

```
delivery.timeout.ms = 120000 (2 min default)

Includes:
  linger.ms          (time waiting to fill batch)
  + network transfer
  + broker processing
  + retry delays (request.timeout.ms × retries)

If total time exceeded → DeliveryTimeoutException

Rule: delivery.timeout.ms ≥ linger.ms + request.timeout.ms
```

<br>

- Controls the **outer time bound** for all retry attempts
- Too low: gives up too quickly on transient failures
- Too high: slow failure detection for callers

---

## Three Configurations for Three Use-Cases

```
Config A — Metrics & Logs (loss acceptable):
  acks=0, linger.ms=0, batch.size=16KB
  Max throughput, zero latency overhead
  Use: click events, telemetry, access logs

Config B — Internal Events (duplicates tolerable):
  acks=1, linger.ms=5ms, batch.size=32KB
  Balanced: good speed with leader confirmation
  Use: internal service events, analytics feeds

Config C — Business Events (no loss, no duplicates):
  acks=all, linger.ms=20ms, batch.size=64KB
  enable.idempotence=true
  Use: orders, payments, financial transactions
```

> Match configuration to business requirements, not technical defaults.

---

## BenchmarkService Results

**Measured with single broker (illustrative):**

```
Config A (acks=0):
  ~5000 msg/s   latency ~0.2ms
  Fire and forget — no waiting

Config B (acks=1):
  ~1000 msg/s   latency ~1.0ms
  Leader round-trip

Config C (acks=all, linger=20ms):
  ~800 msg/s    latency ~1.2ms
  With batching: throughput improves significantly

With 3 brokers and min.insync.replicas=2:
  acks=1 vs acks=all difference becomes more pronounced
  acks=all adds full ISR synchronization latency
```

> For business events: 800 msg/s is entirely acceptable.
> The reliability gain far outweighs the throughput cost.

---

## Key Takeaways

- **`acks=all`** + **`enable.idempotence=true`** = production standard for business events
- **`linger.ms`** + **`batch.size`** = latency vs throughput trade-off knobs
- **Idempotence** prevents broker-level duplicates on retry (≠ consumer-level duplicates)
- **`delivery.timeout.ms`** = total time budget for all send attempts combined
- **`min.insync.replicas=2`** with 3 brokers = tolerates one broker failure
- **ISR** shrinks when followers fall behind → `acks=all` gets stricter automatically
- With single broker: `acks=1` vs `acks=all` performance difference is minimal

<br>

> Choose producer config based on: how bad is data loss? how bad are duplicates?

---

## What's Next — Branch 08

**Consumer Configuration — tuning for production**

- `CooperativeStickyAssignor` — incremental rebalance without stop-the-world
- Static Group Membership — fewer rebalances on rolling restart
- `max.poll.records` — protect against `max.poll.interval.ms` timeout
- `session.timeout.ms` vs `heartbeat.interval.ms` — the rule of three
- Graceful shutdown — zero message loss on deploy

---

<!-- _class: title -->

## What's Next — Branch 08
- CooperativeStickyAssignor: incremental rebalance
- Static membership: stable partition ownership
- Poll tuning: records, intervals, heartbeat
- Graceful shutdown on deploy

# Questions?

**Branch 07 — Producer Configuration**