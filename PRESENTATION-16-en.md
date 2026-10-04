---
marp: true
theme: default
paginate: true
backgroundColor: #ffffff
style: |
  section { font-family: 'Segoe UI', sans-serif; font-size: 28px; }
  section.title { text-align: center; justify-content: center; }
  h1 { color: #1a1a2e; }
  h2 { color: #16213e; border-bottom: 2px solid #0f3460; padding-bottom: 8px; }
  code { background: #f4f4f4; padding: 2px 6px; border-radius: 4px; }
  pre { background: #1e1e1e; color: #d4d4d4; border-radius: 8px; }
  blockquote { border-left: 4px solid #0f3460; padding-left: 16px; color: #555; }
---

<!-- _class: title -->

# Branch 16
# Cluster & Replication

**Apache Kafka for Certification & Production**

---

## Agenda

1. Single broker limitations — why clustering matters
2. Replication Factor — copies across brokers
3. ISR (In-Sync Replicas) — the safety net
4. `acks=all` + `min.insync.replicas` — the durability formula
5. Leader Election — automatic failover
6. KRaft mode — 3 nodes without ZooKeeper
7. Demo: broker failure and recovery
8. RF recommendations per environment
9. Key Takeaways & What's Next

---

## Single Broker — The Limitations

**Everything relies on one process:**

```
Single broker:
  partition-0: leader=broker-1  (only copy)

  broker-1 crashes →
    ALL partitions unavailable
    Producers: LeaderNotAvailableException
    Consumers: no data to read
    Recovery time: minutes to hours (manual restart)

3-broker cluster with RF=3:
  partition-0: leader=broker-1, replicas=[1,2,3]

  broker-1 crashes →
    Controller elects broker-2 or broker-3 as new leader
    Recovery time: 10–30 seconds (automatic)
    Producers and consumers continue with minimal interruption
```

> In production, a single broker is never acceptable for critical topics.

---

## Replication Factor

**RF = how many copies of each partition exist:**

```
topic: 16.orders.created  (partitions=3, replication-factor=3)

  partition-0: leader=broker-1, followers=[broker-2, broker-3]
  partition-1: leader=broker-2, followers=[broker-1, broker-3]
  partition-2: leader=broker-3, followers=[broker-1, broker-2]

All reads and writes go to the LEADER.
Followers pull data from leader asynchronously.
```

<br>

```
Kafka UI → Topics → 16.orders.created → Partitions tab:
  Shows leader, replicas, ISR per partition
```

> RF=3 means data survives the loss of any single broker.

---

## ISR — In-Sync Replicas

**ISR = followers that are synchronized with the leader:**

```
Normal state:
  partition-0: leader=1, replicas=[1,2,3], ISR=[1,2,3]

If broker-3 lags behind (slower than replica.lag.time.max.ms):
  partition-0: leader=1, replicas=[1,2,3], ISR=[1,2]
                                                     ↑
                                           broker-3 removed from ISR

broker-3 catches up → automatically rejoins ISR:
  ISR=[1,2,3]
```

<br>

- ISR is dynamic — changes in real time
- Only ISR members are eligible to become leader
- `replica.lag.time.max.ms` (default 30s) controls ISR membership

---

## acks=all + min.insync.replicas

**The durability formula for production:**

```
Configuration:
  acks=all              → producer waits for ALL ISR to confirm
  min.insync.replicas=2 → minimum 2 ISR required for write to succeed

Behavior:
  ISR=[1,2,3] (3 brokers up)   → write ok ✓  (3 ≥ 2)
  ISR=[1,3]   (1 broker down)  → write ok ✓  (2 ≥ 2)
  ISR=[1]     (2 brokers down) → NotEnoughReplicasException ✗ (1 < 2)

Trade-off:
  min.insync.replicas=1: more availability, less durability (single copy)
  min.insync.replicas=3: max durability, zero tolerance for broker failure
  min.insync.replicas=2: balanced — tolerates 1 broker failure
```

> Production gold standard: **RF=3, min.insync.replicas=2**

---

## Leader Election

**Automatic failover — no human intervention required:**

```
kafka-2 (leader of partition-1) crashes:

  Step 1: KRaft Controller detects missing heartbeats from kafka-2
  Step 2: Controller selects new leader from ISR of partition-1
          → kafka-3 becomes new leader of partition-1
  Step 3: Metadata updated — all clients get new leader info
  Step 4: Producers and consumers retry → connect to kafka-3

Timeline:
  t=0s    kafka-2 goes down
  t=5s    KRaft detects failure
  t=10-30s  new leader elected, clients reconnected
  t=30s   normal operation resumes
```

> Only ISR members can become leader — never an out-of-sync follower.

---

## KRaft Mode — 3 Nodes, No ZooKeeper

**Each node is both broker AND controller:**

```yaml
# kafka-1 configuration:
KAFKA_PROCESS_ROLES: broker,controller
KAFKA_NODE_ID: 1
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093
```

```
KRaft Raft consensus:
  3 controller-eligible nodes
  Quorum = majority = 2 of 3

  1 controller down → 2 remain → quorum maintained → cluster operational
  2 controllers down → 1 remains → no quorum → cluster paused

External ports:
  kafka-1: 9092, kafka-2: 9093, kafka-3: 9094
  bootstrap.servers: kafka-1:9092,kafka-2:9093,kafka-3:9094
```

> KRaft replaced ZooKeeper in Kafka 3.3+ — simpler, faster metadata operations.

---

## Demo — Broker Failure Scenarios

**Four scenarios to test in Kafka UI:**

```bash
# Scenario 1: all 3 brokers up → normal operation
curl -X POST http://localhost:8081/api/orders ...

# Scenario 2: stop one broker → ISR shrinks, but writes continue
docker stop kafka-2
# ISR=[1,3] → acks=all + min.isr=2 → still ok ✓

# Scenario 3: stop two brokers → NotEnoughReplicasException
docker stop kafka-3
# ISR=[1] → 1 < 2 → producer fails ✗

# Scenario 4: restart brokers → ISR restores, replication catches up
docker start kafka-2 && docker start kafka-3
# Followers sync → ISR=[1,2,3] → normal operation ✓
```

> Observe in Kafka UI: Topics → partition ISR changes in real time.

---

## RF Recommendations per Environment

**Match replication to your reliability requirements:**

| Environment | RF | min.insync.replicas | Tolerates |
|------------|-----|---------------------|-----------|
| Development | 1 | 1 | Nothing (no HA) |
| Staging | 2 | 1 | 1 broker failure (reads only) |
| Production | 3 | 2 | 1 broker failure + full write |

<br>

```
Cost of RF=3 with 3 brokers:
  3× storage per partition
  More network traffic (replication)
  Slightly higher write latency (acks=all waits for ISR)

The tradeoff is worth it:
  Without RF=3 — one broker crash = data loss
```

---

## Key Takeaways

- **Replication Factor** = number of partition copies across brokers
- **ISR** = dynamic set of synchronized replicas — only ISR can become leader
- **`acks=all` + `min.insync.replicas=2`** = the production durability formula
- **Leader election** = automatic failover in 10–30 seconds
- **KRaft** = 3-node Raft consensus, no ZooKeeper dependency
- **RF=3, min.isr=2** = tolerates 1 broker failure with full write guarantees
- **`bootstrap.servers`** with all brokers = client resilience to single broker failure

<br>

> High availability in Kafka is achieved through replication, not through hardware redundancy.

---

## What's Next — Branch 17

**Observability: Prometheus + Grafana + kafka-exporter**

- `kafka-exporter` — Prometheus scrape endpoint for Kafka metrics
- `kafka_consumergroup_lag` — the primary consumer health metric
- Grafana dashboards with auto-provisioning
- Simulate lag: pause consumer, flood topic, observe dashboard
- `max.poll.records=1` — fine-grained lag control for demos

---

<!-- _class: title -->

## What's Next — Branch 17
- kafka-exporter: Prometheus metrics for Kafka
- Grafana: auto-provisioned dashboards
- Consumer lag: pause, flood, observe
- PromQL: lag queries and alerting rules

# Questions?

**Branch 16 — Cluster & Replication**