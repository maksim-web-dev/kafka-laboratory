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

# Branch 17
# Observability: Prometheus & Grafana

**Apache Kafka for Certification & Production**

---

## Agenda

1. Why Consumer Lag is the primary Kafka health metric
2. Monitoring stack: kafka-exporter → Prometheus → Grafana
3. Key metrics exposed by `kafka-exporter`
4. PromQL queries for lag monitoring
5. Grafana auto-provisioning — datasources and dashboards
6. `max.poll.records=1` — fine-grained lag control
7. Demo: pause → flood → observe → resume
8. Consumer control API: pause / resume / slow / fast
9. Key Takeaways & What's Next

---

## Consumer Lag — The Primary Health Metric

**Lag = how far behind the consumer is:**

```
Producer:  ──●──●──●──●──●──●──●──●──●──  offset 100
Consumer:  ──●──●──●──●──●──●─────────    offset  75
                                    ↑↑↑
                              LAG = 25

Consumer lag = latest_offset - committed_offset

LAG = 0   → consumer processes in real time ✓
LAG > 0   → consumer is behind (stopped, slow, or overloaded)
LAG grows → consumer cannot keep up with producer → alert!
```

<br>

> Growing lag = delayed business logic. A notification arriving 10 minutes late is a bug.

---

## Monitoring Stack

**Each component has a single responsibility:**

```
Kafka broker
     │  JMX metrics (internal)
     ▼
kafka-exporter (:9308/metrics)
     │  Prometheus text format
     ▼
Prometheus (:9090)
     │  scrape every 15s → time-series storage
     ▼
Grafana (:3000)
     │  PromQL queries → dashboards, alerts
     ▼
Engineer (alert → action)
```

<br>

- `kafka-exporter` = sidecar that translates Kafka metrics to Prometheus format
- Prometheus scrapes `/metrics` endpoint on schedule
- Grafana visualizes via PromQL queries

---

## Key Metrics from kafka-exporter

**The most important metrics for consumer health:**

```
kafka_consumergroup_lag
  → consumer group lag per (group, topic, partition)

kafka_consumergroup_current_offset
  → committed offset per (group, topic, partition)

kafka_topic_partition_current_offset
  → latest offset per partition (producer position)

kafka_brokers
  → number of alive brokers in the cluster

kafka_topic_partitions
  → partition count per topic
```

<br>

> These 5 metrics cover 90% of production Kafka monitoring needs.

---

## PromQL — Lag Queries

**Query language for Prometheus — used in Grafana panels:**

```sql
-- Current lag for ALL consumer groups
kafka_consumergroup_lag

-- Lag for a specific group
kafka_consumergroup_lag{
  consumergroup="notification-service-group"
}

-- Total lag summed across all partitions for a group
sum(kafka_consumergroup_lag) by (consumergroup)

-- Producer throughput (messages per second)
rate(kafka_topic_partition_current_offset[1m])

-- Alert rule: lag > 100 for more than 5 minutes
kafka_consumergroup_lag > 100
```

---

## Grafana Auto-Provisioning

**No manual dashboard setup — everything loaded from config files:**

```
grafana/
├── provisioning/
│   ├── datasources/
│   │   └── prometheus.yaml    ← auto-connect Prometheus as datasource
│   └── dashboards/
│       └── dashboards.yaml    ← tell Grafana where to find JSON dashboards
└── dashboards/
    └── kafka-lag.json         ← 7 panels (loaded automatically on startup)
```

<br>

**Dashboard panels:**
- Consumer Lag per partition (time series)
- Latest offset vs committed offset
- Consumer group status (active/inactive)
- Broker count

> Infrastructure-as-code for dashboards: commit JSON, done.

---

## max.poll.records=1

**Why single-record polling makes lag visible in real time:**

```yaml
# notification-service/application.yml
spring:
  kafka:
    consumer:
      properties:
        max.poll.records: 1          # poll one message at a time
        max.poll.interval.ms: 600000 # allow 10 min before kick-out (for pause demo)
```

```
Without max.poll.records=1:
  poll() → 500 messages → process all → commit 500 at once
  Lag decreases by 500 in one step → hard to observe on dashboard

With max.poll.records=1:
  poll() → 1 message → process → commit → poll() again
  Lag decreases by 1 each time → smooth curve on Grafana ✓
```

---

## Consumer Control API

**Simulate real-world consumer states via REST:**

```bash
# Pause consumer — stop reading, kafka-exporter shows growing lag
curl -X POST http://localhost:8082/api/consumer/pause

# Generate 20 orders while consumer is paused
curl "http://localhost:8081/api/orders/flood?count=20"
# → Grafana shows lag climbing from 0 to 20

# Resume — consumer catches up, lag drops to 0
curl -X POST http://localhost:8082/api/consumer/resume

# Slow mode — process 1 message per 2 seconds (lag grows slowly)
curl -X POST http://localhost:8082/api/consumer/slow

# Fast mode — back to normal speed
curl -X POST http://localhost:8082/api/consumer/fast
```

> These 4 states simulate common production scenarios: deploy, overload, recovery.

---

## Demo — Pause → Flood → Observe → Resume

**Step-by-step lag demonstration:**

```
Step 1: Open Grafana at http://localhost:3000 (admin/admin)
        → navigate to "Kafka Consumer Lag" dashboard

Step 2: Pause the consumer
        curl -X POST http://localhost:8082/api/consumer/pause

Step 3: Flood the topic with 30 orders
        curl "http://localhost:8081/api/orders/flood?count=30"
        → Watch Grafana: lag climbs from 0 → 30 over ~15 seconds

Step 4: Resume
        curl -X POST http://localhost:8082/api/consumer/resume
        → Watch Grafana: lag drops from 30 → 0 as consumer catches up

Step 5: Try "slow" mode — lag grows slowly while consumer processes
```

> This demo shows exactly what production on-call engineers watch during incidents.

---

## Key Takeaways

- **Consumer Lag** = `latest_offset - committed_offset` = primary Kafka health metric
- **kafka-exporter** = sidecar translating Kafka metrics to Prometheus format
- **Prometheus** = time-series database with 15s scrape interval
- **Grafana provisioning** = dashboards-as-code, no manual setup
- **PromQL** = `kafka_consumergroup_lag{group="..."}` for targeted monitoring
- **`max.poll.records=1`** = fine-grained lag visibility on dashboards
- **Alert rule**: `sum(kafka_consumergroup_lag) > threshold` → PagerDuty

<br>

> You cannot manage what you cannot measure. Lag is the heartbeat of Kafka.

---

## What's Next — Branch 18

**Security: SASL/PLAIN + ACL**

- SASL/PLAIN authentication — username/password per Kafka client
- ACL (Access Control Lists) — who can read/write which topic
- `StandardAuthorizer` — built-in KRaft authorizer
- `ALLOW_EVERYONE_IF_NO_ACL_FOUND=false` — deny by default
- Super Users — `User:admin` bypasses all ACL checks

---

<!-- _class: title -->

## What's Next — Branch 18
- SASL/PLAIN: username + password for each client
- ACL: declarative per-topic, per-user permissions
- Deny by default: least privilege principle
- acl-init: automated ACL setup at startup

# Questions?

**Branch 17 — Observability: Prometheus & Grafana**