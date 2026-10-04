# Branch 16 — Cluster & Replication

## 1. What we learn in this branch

- **Multi-broker cluster** — 3 Kafka brokers in a single Docker Compose (KRaft mode, no ZooKeeper).
- **Replication Factor (RF)** — how many copies of each partition are stored in the cluster.
- **ISR (In-Sync Replicas)** — the subset of replicas that are synchronized with the leader in real time.
- **Leader Election** — when a broker fails, Kafka automatically elects a new leader from ISR.
- **`min.insync.replicas`** — minimum number of in-sync replicas required to accept a write with `acks=all`.
- **`NotEnoughReplicasException`** — what happens when ISR < min.insync.replicas.
- **4 demo scenarios**: normal operation, one broker down, two brokers down, recovery.

---

## 2. Changes compared to the previous branch (branch15)

- **Instead of one** Kafka broker, now **3 broker/controller** nodes (kafka-1, kafka-2, kafka-3).
- **Restored** notification-service as a simple consumer.
- **Removed** Kafka Connect, PostgreSQL, Elasticsearch — focus is on infrastructure.
- **Topic** `16.orders.created`: partitions=3, replication-factor=3, min.insync.replicas=2.
- **order-service** received `/api/cluster/brokers` and `/api/cluster/topic-info` endpoints.
- **bootstrap-servers**: now contains all 3 brokers comma-separated.

---

## 3. Architecture

```
order-service (acks=all)
  bootstrap: kafka-1:9092,kafka-2:9092,kafka-3:9092
      │
      ▼
┌──────────────────────────────────────────┐
│    Kafka Cluster (KRaft, 3 nodes)        │
│                                          │
│  kafka-1 (broker + controller) [leader?] │
│  kafka-2 (broker + controller)           │
│  kafka-3 (broker + controller)           │
│                                          │
│  topic: 16.orders.created                │
│    partition-0: leader=1, ISR=[1,2,3]    │
│    partition-1: leader=2, ISR=[1,2,3]    │
│    partition-2: leader=3, ISR=[1,2,3]    │
└──────────────────────────────────────────┘
      │
      ▼
notification-service (consumer group)
```

**Services and ports (from docker-compose-16.yml):**
- `kafka-1` — 9092 (external)
- `kafka-2` — 9093 (external)
- `kafka-3` — 9094 (external)
- `kafka-ui` — 8080 → http://localhost:8080
- `order-service-b16` — 8081 → http://localhost:8081
- `notification-service-b16` — 8082 → http://localhost:8082

---

## 4. Key Concepts

### Replication Factor and ISR

```
Partition 0 with RF=3:
  Leader:   broker-1  ← all reads/writes go to the leader
  Replicas: [1, 2, 3] ← list of all replicas
  ISR:      [1, 2, 3] ← In-Sync: those not lagging > replica.lag.time.max.ms

Followers asynchronously pull data from the leader.
If a follower falls behind → it is removed from ISR.
```

### acks=all + min.insync.replicas

```
acks=all → producer waits for acknowledgement from ALL ISR (not just the leader)

min.insync.replicas=2 → minimum number of ISR replicas for a successful write

Combination:
  ISR=[1,2,3]: write ok (3 >= 2) ✓
  ISR=[1,3]:   write ok (2 >= 2) ✓ (one broker can fail)
  ISR=[1]:     NotEnoughReplicasException ✗ (1 < 2)
```

### Leader Election

```
kafka-2 (leader of partition-1) fails:
  1. Kafka controller detects: kafka-2 is not sending heartbeats
  2. Controller elects a new leader from the ISR of partition-1
     (e.g., kafka-3 becomes the leader of partition-1)
  3. Metadata is updated — clients receive the new leader
  4. Failover time: ~10–30 seconds (depends on session.timeout)

Important: only a node from ISR can become the leader (not any replica!)
```

### KRaft: 3 broker + controller per node

```yaml
# kafka-1 environment
KAFKA_PROCESS_ROLES: broker,controller
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093
```

Each node is both a broker (serves clients) and a controller (KRaft Raft-based metadata).
Quorum: 2 of 3 controllers are needed for metadata decisions.

### Replication Factor recommendations

```
Environment   │ RF │ min.insync.replicas │ Tolerates
──────────────┼────┼─────────────────────┼────────────────────
Development   │  1 │ 1                   │ Nothing (no HA)
Staging       │  2 │ 1                   │ 1 broker failure
Production    │  3 │ 2                   │ 1 broker failure + write
```

---

## 5. How to Run

```bash
# Start 3-broker cluster
docker compose -f docker-compose-16.yml up --build

# Kafka UI: http://localhost:8080
```

**Cluster check:**
```bash
# Broker state via order-service API
curl http://localhost:8081/api/cluster/brokers | jq .
# [{"brokerId":1,...}, {"brokerId":2,...}, {"brokerId":3,...}]

# Replication state
curl http://localhost:8081/api/cluster/topic-info | jq .
# partition-0: leader=1, isr=[1,2,3], underReplicated=false
```

---

## 6. How to Test

### Demo 1 — Normal operation (3/3 ISR)

```bash
# ISR is full: [1, 2, 3] for every partition
curl http://localhost:8081/api/cluster/topic-info | jq .

# Send orders — all succeed
curl -X POST "http://localhost:8081/api/orders/batch?users=alice,bob&count=5"
# {"requested":5,"sent":5,"failed":0}

# Kafka UI: http://localhost:8080 → Topics → 16.orders.created → Partitions
```

### Demo 2 — One broker down (2/3 ISR)

```bash
# Stop kafka-2
docker compose -f docker-compose-16.yml stop kafka-2

# ISR: [1, 3] — under-replicated, but 2 >= min.insync.replicas=2
curl http://localhost:8081/api/cluster/topic-info | jq .
# underReplicated: true, but leader election succeeded

# Orders CONTINUE to succeed
curl -X POST "http://localhost:8081/api/orders/batch?users=alice,bob&count=5"
# {"requested":5,"sent":5,"failed":0}
```

### Demo 3 — Two brokers down (1/3 ISR < min.insync.replicas)

```bash
# Stop kafka-3 (kafka-2 is already stopped)
docker compose -f docker-compose-16.yml stop kafka-3

# ISR: [1] — only 1 < min.insync.replicas=2
curl http://localhost:8081/api/cluster/topic-info | jq .

# ERROR! NotEnoughReplicasException
curl -X POST "http://localhost:8081/api/orders/batch?users=alice,bob&count=5"
# {"requested":5,"sent":0,"failed":5}
# Log: NotEnoughReplicasException: fewer in-sync replicas than required
```

### Demo 4 — Recovery

```bash
# Start both brokers
docker compose -f docker-compose-16.yml start kafka-2 kafka-3

# Wait ~30s for sync
sleep 30
curl http://localhost:8081/api/cluster/topic-info | jq .
# ISR=[1,2,3], underReplicated=false

# Production resumes
curl -X POST "http://localhost:8081/api/orders/batch?users=alice,bob&count=5"
# {"requested":5,"sent":5,"failed":0}
```

---

## 7. Deep Dive

### How does Kafka decide who is the leader?

```
On cluster start:
  Controller assigns "preferred leader" for each partition
  Preferred leader = first replica in the list (usually broker-id % num_brokers)

On leader failure:
  Controller picks the first replica from ISR (not the preferred leader!)
  If ISR is empty → unclean.leader.election.enable=false → no election
                   → unclean.leader.election.enable=true  → elect from outside ISR
                     (risk of data loss!)

"Leader rebalance" (auto.leader.rebalance.enable=true):
  After recovery — preferred leader automatically reclaims its role
```

### Why do bootstrap-servers contain all 3 brokers?

```yaml
SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka-1:9092,kafka-2:9092,kafka-3:9092
```

Bootstrap servers are only for initial connection and metadata retrieval.
If kafka-1 is unavailable, the client tries kafka-2, then kafka-3.
After receiving metadata, the client connects directly to the partition leaders.

### Internal topics RF

```yaml
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
```

`__consumer_offsets` and `__transaction_state` also have RF=3.
Without this, consumer group coordination may break when a broker fails.

---

## 8. Project Structure

```
branch16_cluster_replication/
├── order-service/
│   ├── controller/
│   │   ├── OrderController.kt     ← POST /api/orders, POST /api/orders/batch
│   │   └── ClusterController.kt   ← GET /api/cluster/brokers, GET /api/cluster/topic-info
│   ├── service/
│   │   ├── OrderService.kt
│   │   └── ClusterInfoService.kt  ← AdminClient for metadata
│   └── config/KafkaTopicConfig.kt ← topic with RF=3, min.insync.replicas=2
└── notification-service/
    └── listener/OrderEventListener.kt
```

---

## 9. What's Next

- **Branch 17** — Observability: Prometheus + Grafana + kafka-exporter; consumer lag monitoring.
- **Branch 19** — Production-like: 3-broker cluster + SASL/ACL + Schema Registry + Saga.

---

## 10. Lecture Slides

### Slide 1 — Replication Factor

```
Partition 0, RF=3:

  Broker-1 [LEADER]  ◄── Producer writes here
      │
      ├──► Broker-2 [FOLLOWER]  (async replication)
      └──► Broker-3 [FOLLOWER]  (async replication)

ISR = {1, 2, 3}  ← all synchronized
```

### Slide 2 — min.insync.replicas

```
acks=all + min.insync.replicas=2:

3 brokers up   → ISR=[1,2,3] → write ok  ✓
1 broker down  → ISR=[1,3]   → write ok  ✓ (2 >= 2)
2 brokers down → ISR=[1]     → EXCEPTION ✗ (1 < 2)

Trade-off: durability vs availability
  min.isr=1: more availability, less durability
  min.isr=3: more durability, less availability
```

### Slide 3 — Leader Election

```
kafka-2 (leader partition-1) ✗ FALLS

           KRaft Controller detects failure
                     │
                     ▼
           New leader from ISR: kafka-3
                     │
              Clients get new metadata
                     │
          Write continues to kafka-3
```

---

## 11. Demo Script

```bash
#!/bin/bash
echo "=== Branch 16: Cluster & Replication Demo ==="

docker compose -f docker-compose-16.yml up --build -d
sleep 60

echo ""
echo "=== Cluster state (3/3 brokers) ==="
curl -s http://localhost:8081/api/cluster/brokers | jq '.[].brokerId'
curl -s http://localhost:8081/api/cluster/topic-info | jq .[0]

echo ""
echo "=== Send with 3 brokers ==="
curl -s -X POST "http://localhost:8081/api/orders/batch?users=alice,bob&count=5" | jq .

echo ""
echo "=== Stop kafka-2 ==="
docker compose -f docker-compose-16.yml stop kafka-2
sleep 15
echo "ISR after stop:"
curl -s http://localhost:8081/api/cluster/topic-info | jq '.[].isr'
echo "Send with 2 brokers:"
curl -s -X POST "http://localhost:8081/api/orders/batch?users=alice,bob&count=5" | jq .

echo ""
echo "=== Stop kafka-3 ==="
docker compose -f docker-compose-16.yml stop kafka-3
sleep 15
echo "ISR after 2 stops:"
curl -s http://localhost:8081/api/cluster/topic-info | jq '.[].isr'
echo "Send with 1 broker (expecting ERROR):"
curl -s -X POST "http://localhost:8081/api/orders/batch?users=alice,bob&count=5" | jq .

echo ""
echo "=== Recovery ==="
docker compose -f docker-compose-16.yml start kafka-2 kafka-3
sleep 30
echo "ISR after recovery:"
curl -s http://localhost:8081/api/cluster/topic-info | jq '.[].isr'
curl -s -X POST "http://localhost:8081/api/orders/batch?users=alice,bob&count=5" | jq .
```

---

## 12. Self-Check Questions

- What is ISR and how does a broker enter or leave ISR?
- What is the difference between `replication-factor` and `min.insync.replicas`?
- What happens when the partition leader fails?
- Why are multiple brokers listed in bootstrap-servers?
- Under what conditions does `NotEnoughReplicasException` occur?
- What is `unclean.leader.election.enable` and when should it be enabled?
- Why do internal topics (`__consumer_offsets`) also have RF=3?
- How many brokers can fail with RF=3, min.isr=2 for the system to keep working?