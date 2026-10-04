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

# Branch 19
# Production-like Demo

**Apache Kafka for Certification & Production**

---

## Agenda

1. What "production-ready" means for Kafka
2. All patterns combined — the feature pyramid
3. Architecture: 4 services, 3 brokers, 9 topics
4. Production checklist — every item checked
5. SASL users and ACL matrix (5 users)
6. Topics: RF=3, partitions=3, min.isr=2
7. branch13 vs branch19 — what changed
8. Grafana dashboard — DLT counter + lag
9. Key Takeaways — end of the core course

---

## What "Production-Ready" Means

**A production Kafka system is not just one feature — it's a combination:**

```
Development (branch01-05):
  Single broker, no auth, no monitoring, happy path only

Production (branch19):
  ✓ 3-broker cluster (HA)
  ✓ Replication Factor=3, min.insync.replicas=2
  ✓ SASL/PLAIN + ACL (authentication + authorization)
  ✓ Avro + Schema Registry (schema governance)
  ✓ Idempotent producer (no duplicate messages)
  ✓ Manual commit (no message loss on crash)
  ✓ Dead Letter Topic (poison message handling)
  ✓ Saga choreography (distributed transactions)
  ✓ Prometheus + Grafana (observability)
  ✓ AUTO_CREATE_TOPICS=false (controlled topology)
```

---

## The Feature Pyramid

**Branch19 combines everything from the course:**

```
              [branch19 — Production-like Demo]
             /          |          |          \
       [branch18]  [branch16]  [branch17]  [branch11]
       SASL/ACL    3-broker    Monitoring  Schema Reg.
           |            |           |          |
       [branch13]  [branch12]  [branch09]  [branch06]
       Saga        Transactions  DLT       Manual Commit
           |            |           |          |
       [branch05-10]   core consumer/producer patterns
```

> Each branch is a building block. Branch19 assembles them into a real system.

---

## Architecture — 4 Services, 3 Brokers, 9 Topics

```
order-service (idempotent, acks=all)
  ──[19.orders.created]────────────► payment-service (idempotent)
                                           │
                          OK → [19.payments.processed]
                        FAIL → [19.payments.failed] ──► order-service
                                           │
                                    inventory-service (manual commit)
                                           │
                        OK → [19.inventory.reserved] ──► order-service
                       FAIL → [19.inventory.failed] ──► payment-service
                                                    └──► order-service

notification-service (read_committed, DLT):
  listens to ALL 8 topics + DLT (19.orders.created.dlt)

Kafka Cluster: kafka-b19-1:9122, kafka-b19-2:9123, kafka-b19-3:9124
```

---

## Production Checklist

**Every item from the course applied:**

```
✓ acks=all + min.insync.replicas=2 + replication-factor=3
✓ enable.idempotence=true     (order-service, payment-service)
✓ Manual commit + MANUAL_IMMEDIATE  (inventory-service)
✓ Dead Letter Topic           (notification-service)
✓ Consumer lag alerting       (Grafana + kafka-exporter)
✓ SASL/PLAIN + ACL            (least privilege per service)
✓ Avro + Schema Registry      (breaking change prevention)
✓ Saga choreography           (distributed transactions)
✓ isolation.level=read_committed  (notification-service)
✓ AUTO_CREATE_TOPICS_ENABLE=false
✓ Retention policy per topic
✓ init-topics container       (topic + ACL as code)
```

---

## SASL Users and ACL Matrix

**5 users — each with minimal permissions:**

```
User                  Credentials         WRITE                READ
admin                 admin-secret        all (super user)     all
order-producer        order-secret        19.orders.created    19.payments.failed
                                                               19.inventory.*
payment-processor     payment-secret      19.payments.*        19.orders.created
                                                               19.inventory.failed
inventory-worker      inventory-secret    19.inventory.*       19.payments.processed
notification-consumer notif-secret        19.orders.created.dlt  all 19.* topics
```

<br>

> Each service authenticates with its own credentials.
> No service can read or write outside its own responsibility.

---

## Topics — RF=3, partitions=3, min.isr=2

**All 9 topics with production-grade replication:**

```
19.orders.created           ← saga init (3 partitions, RF=3)
19.payments.processed       ← payment ok
19.payments.failed          ← payment fail → compensation
19.payments.refunded        ← inventory fail → refund
19.inventory.reserved       ← inventory ok
19.inventory.failed         ← inventory fail → compensation
19.orders.confirmed         ← saga complete
19.orders.cancelled         ← saga cancelled (with reason)
19.orders.created.dlt       ← Dead Letter Topic (1 partition)
```

<br>

> `AUTO_CREATE_TOPICS_ENABLE=false` — topics only exist if explicitly created by `init-topics`.

---

## Branch13 vs Branch19 — What Changed

**The same business logic, production-hardened:**

```
                   branch13              branch19
Brokers            1                     3 (KRaft, RF=3)
Security           none                  SASL/PLAIN + ACL
min.insync.rep.    1                     2
Producer           standard              idempotent
Commit             auto-commit           manual (inventory-service)
DLT                none                  DeadLetterPublishingRecoverer
Monitoring         none                  Prometheus + Grafana
Schema Registry    port 8090             port 8191
Consumer isolation read_uncommitted      read_committed (notification)
Topics             8                     9 (+ DLT)
```

---

## Grafana Dashboard — DLT Counter + Lag

**What to observe in branch19 Grafana (port 3019):**

```
Dashboard panels:
  1. Consumer Lag per group
     → 5 groups: order, payment, inventory, notification, init-topics

  2. DLT message counter
     → how many messages landed in 19.orders.created.dlt

  3. Broker count
     → should always be 3; alert if < 3

PromQL for DLT monitoring:
  kafka_topic_partition_current_offset{
    topic="19.orders.created.dlt"
  }

Simulate DLT:
  POST /api/notifications/simulate-failure/5
  → notification exhausts retries → DLT counter increments
```

---

## Key Takeaways

- **Production Kafka** = multiple patterns working together, not a single feature
- **The pyramid**: basic patterns → error handling → security → HA → observability
- **Branch19 checklist** = reference configuration for any new Kafka project
- **5 SASL users, 9 topics, 3 brokers** = realistic microservice topology
- **init-topics container** = topic + ACL creation as code (reproducible)
- **Idempotent + manual commit + DLT** = complete message delivery guarantee
- **Grafana** = DLT counter + lag graph = everything on-call needs

<br>

> Branch19 is where all the pieces come together. Reference it when designing new systems.

---

## What's Next — Branch 20

**Kafka Streams Advanced: Joins, GlobalKTable, EOS**

- `KStream-KTable Join` — enrich stream with table data (co-partitioned)
- `KStream-GlobalKTable Join` — enrich without co-partitioning requirement
- `KStream-KStream Windowed Join` — correlate two streams in time window
- `selectKey()` — re-key stream for co-partitioning
- `processing.guarantee=exactly_once_v2` — EOS in Kafka Streams

---

<!-- _class: title -->

## What's Next — Branch 20
- Three join types: KTable, GlobalKTable, KStream-KStream
- selectKey: re-keying for co-partitioning
- Windowed joins: correlate events in time
- EOS: exactly_once_v2 in Kafka Streams

# Questions?

**Branch 19 — Production-like Demo**