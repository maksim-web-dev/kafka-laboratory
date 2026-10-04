# Branch 17 — Observability: Prometheus + Grafana + kafka-exporter

## 1. What we learn in this branch

- **Consumer Lag** — the key Kafka metric: how far the consumer is behind the producer.
- **kafka-exporter** — sidecar service that collects metrics from the Kafka broker and converts them to
  Prometheus format.
- **Prometheus** — system for collecting and storing time-series metrics.
- **Grafana** — dashboards for visualization; provisioning via configuration files.
- **pause / resume / slow** API — simulating different consumer states to observe lag.
- **`max.poll.records=1`** — why a small value is useful for demonstrating lag.

---

## 2. Changes compared to the previous branch (branch16)

- **Reverted** to single-broker (instead of 3-broker cluster) — focus is on observability, not HA.
- **Added** kafka-exporter, prometheus, grafana.
- **New endpoint** `/api/orders/flood?count=N` for rapidly generating lag.
- **New API** in notification-service: `/api/consumer/pause`, `/resume`, `/slow`, `/fast`.
- **`max.poll.records=1`** in notification-service — lag changes one message at a time.
- **Grafana provisioning**: datasource + dashboard load automatically.

---

## 3. Architecture

```
order-service ──► Kafka (17.orders.created) ──► notification-service
                        │
                  kafka-exporter (/metrics)
                        │
                  Prometheus ──► Grafana (dashboard)
```

**Metrics flow:**
```
Kafka broker → kafka-exporter :9308 → Prometheus :9090 → Grafana :3000
```

**Services and ports (from docker-compose-17.yml):**
- `kafka` — 9092 (KRaft single-node)
- `kafka-ui` — 8080 → http://localhost:8080
- `kafka-exporter-b17` — :9308 (Prometheus scrape endpoint, no external port)
- `prometheus-b17` — 9090 → http://localhost:9090
- `grafana-b17` — 3000 → http://localhost:3000 (admin/admin)
- `order-service-b17` — 8081 → http://localhost:8081
- `notification-service-b17` — 8082 → http://localhost:8082

---

## 4. Key Concepts

### Consumer Lag

```
Consumer lag = Latest Offset − Consumer Offset

topic: 17.orders.created
  partition-0:
    latest offset:   100  ← producer wrote 100 messages
    consumer offset: 75   ← consumer processed 75
    LAG:             25   ← 25 messages are waiting to be processed

Lag > 0 means: consumer is not keeping up with the producer or is temporarily paused.
Lag → 0: consumer has processed all messages.

kafka_consumergroup_lag{topic="17.orders.created", partition="0"} 25
```

### kafka-exporter metrics

```
kafka_consumergroup_lag              ← lag per partition
kafka_consumergroup_current_offset   ← consumer's current offset
kafka_topic_partition_current_offset ← latest offset in the topic
kafka_brokers                        ← number of live brokers
kafka_topic_partitions               ← number of partitions

Scrape interval: 15s (in prometheus.yml)
```

### max.poll.records=1

```yaml
# notification-service/application.yml
spring:
  kafka:
    consumer:
      properties:
        max.poll.records: 1
        max.poll.interval.ms: 600000
```

- `max.poll.records=1` — consumer fetches 1 message per poll.
  Lag increases/decreases by 1 at a time → clearly visible on Grafana.
- `max.poll.interval.ms=600000` — if the consumer is "paused" (`pause` API), Kafka
  does not remove it from the group for up to 10 minutes.

### Grafana Provisioning

```
grafana/provisioning/
├── datasources/
│   └── prometheus.yaml  ← auto-connect Prometheus as datasource
└── dashboards/
    └── dashboards.yaml  ← where to find JSON dashboards

grafana/dashboards/
└── kafka-lag.json       ← 7 panels (loaded automatically)
```

---

## 5. How to Run

```bash
docker compose -f docker-compose-17.yml up --build

# Kafka UI:   http://localhost:8080
# Prometheus: http://localhost:9090
# Grafana:    http://localhost:3000  (admin/admin)
```

**Verification:**
```bash
# Confirm kafka-exporter is collecting metrics
curl -s http://localhost:9090/api/v1/query \
  --data-urlencode 'query=kafka_brokers' | jq .data.result[0].value[1]
# "1"
```

---

## 6. How to Test

### Demo 1 — Basic lag

```bash
# Pause the consumer
curl -X POST http://localhost:8082/api/consumer/pause

# Send 100 messages
curl -X POST "http://localhost:8081/api/orders/flood?count=100"

# Grafana → "Kafka Observability" dashboard
# Consumer Lag: ~100
# Lag Over Time: rising chart
```

### Demo 2 — Consumer recovery (lag → 0)

```bash
curl -X POST http://localhost:8082/api/consumer/resume

# Grafana → lag gradually drops to 0
```

### Demo 3 — Slow consumer

```bash
# Process 1 message every 3 seconds
curl -X POST "http://localhost:8082/api/consumer/slow?ms=3000"

# Send 30 messages
curl -X POST "http://localhost:8081/api/orders/flood?count=30"

# Grafana → lag grows (producer is faster than consumer)
# After ~90s: consumer catches up, lag → 0

# Return to normal speed
curl -X POST http://localhost:8082/api/consumer/fast
```

### Demo 4 — Prometheus UI

```bash
# Open: http://localhost:9090
# Graph → query:
#   kafka_consumergroup_lag
#   kafka_consumergroup_lag{consumergroup="notification-service-group"}
#   sum(kafka_consumergroup_lag) by (consumergroup)
```

---

## 7. Deep Dive

### Grafana Dashboard — 7 panels

```
1. Consumer Lag (total)         ← sum(kafka_consumergroup_lag) across all partitions
2. Messages in Topic            ← kafka_topic_partition_current_offset
3. Consumer Offset              ← kafka_consumergroup_current_offset
4. Active Brokers               ← kafka_brokers
5. Consumer Lag Over Time       ← lag per partition over time (line chart)
6. Producer vs Consumer Offset  ← gap between them = lag (dual-axis chart)
7. Production Rate              ← rate(kafka_topic_partition_current_offset[1m])
```

### Prometheus scrape configuration

```yaml
# prometheus.yml
scrape_configs:
  - job_name: kafka-exporter
    static_configs:
      - targets: ['kafka-exporter-b17:9308']
    scrape_interval: 15s

  - job_name: prometheus
    static_configs:
      - targets: ['localhost:9090']
```

### Consumer pause/resume implementation

```kotlin
// notification-service
private val container: MessageListenerContainer

fun pause() {
    container.pause()    // Kafka consumer.pause(partitions) — stops polling
}

fun resume() {
    container.resume()   // Kafka consumer.resume(partitions)
}

fun slow(ms: Long) {
    processingDelayMs = ms   // Thread.sleep(ms) before ack
}
```

`container.pause()` does not stop the JVM thread; it instructs the consumer not to fetch new poll batches.
Offset is not committed → lag accumulates.

---

## 8. Project Structure

```
branch17_observability/
├── prometheus/
│   └── prometheus.yml              ← scrape config (kafka-exporter)
├── grafana/
│   ├── provisioning/
│   │   ├── datasources/
│   │   │   └── prometheus.yaml     ← auto-provision Prometheus datasource
│   │   └── dashboards/
│   │       └── dashboards.yaml     ← auto-provision dashboard JSON
│   └── dashboards/
│       └── kafka-lag.json          ← 7-panel dashboard
├── order-service/
│   └── controller/OrderController.kt ← POST /api/orders/flood?count=N
└── notification-service/
    ├── controller/ConsumerController.kt ← /pause, /resume, /slow, /fast
    └── listener/OrderEventListener.kt   ← max.poll.records=1, configurable delay
```

---

## 9. What's Next

- **Branch 18** — Security: SASL/PLAIN authentication and ACL (Access Control Lists).
- **Branch 19** — Production-like: monitoring + cluster + security + saga combined.

---

## 10. Lecture Slides

### Slide 1 — Consumer Lag

```
Producer:  ──●──●──●──●──●──●──●──●──●──  (offset 100)
Consumer:  ──●──●──●──●──●──●─────────    (offset 75)
                                    ↑↑↑
                                   LAG=25

Consumer lag = delay in messages.
Critical lag → delayed business logic → alerts!
```

### Slide 2 — Monitoring Stack

```
Kafka broker
     │
kafka-exporter ─► /metrics (Prometheus format)
     │
Prometheus ────── scrape every 15s → time-series DB
     │
Grafana ────────── PromQL queries → dashboards, alerts
```

### Slide 3 — PromQL Queries

```sql
-- Current lag
kafka_consumergroup_lag

-- Lag by group
kafka_consumergroup_lag{consumergroup="notification-service-group"}

-- Production rate (messages/sec)
rate(kafka_topic_partition_current_offset[1m])

-- Total lag
sum(kafka_consumergroup_lag) by (consumergroup)
```

---

## 11. Demo Script

```bash
#!/bin/bash
echo "=== Branch 17: Observability Demo ==="

docker compose -f docker-compose-17.yml up --build -d
sleep 60

echo ""
echo "=== Check metrics ==="
curl -s "http://localhost:9090/api/v1/query?query=kafka_brokers" | jq .data.result[0].value[1]

echo ""
echo "=== Step 1: Pause consumer ==="
curl -X POST http://localhost:8082/api/consumer/pause

echo ""
echo "=== Step 2: Flood 100 messages ==="
curl -s -X POST "http://localhost:8081/api/orders/flood?count=100"
echo "Open Grafana: http://localhost:3000"
echo "Dashboard: Kafka Observability"
echo "Lag should be ~100"
sleep 20

echo ""
echo "=== Step 3: Resume consumer ==="
curl -X POST http://localhost:8082/api/consumer/resume
echo "Lag is dropping..."
sleep 15

echo ""
echo "=== Step 4: Slow consumer + flood ==="
curl -X POST "http://localhost:8082/api/consumer/slow?ms=2000"
curl -s -X POST "http://localhost:8081/api/orders/flood?count=20"
echo "Lag grows (consumer slower than producer)"
sleep 30
curl -X POST http://localhost:8082/api/consumer/fast
echo "Consumer caught up"
```

---

## 12. Self-Check Questions

- What is consumer lag and how is it calculated?
- What role does kafka-exporter play in the monitoring stack?
- Why is `max.poll.records=1` useful for demonstrating lag?
- What happens to the consumer group on `container.pause()` with `max.poll.interval.ms=600000`?
- What is the difference between `consumer offset` and `topic partition current offset`?
- Why is PromQL `rate()` useful for tracking throughput?
- What is Grafana provisioning and why is it better than manual configuration?
- Which alert should be configured first in a production Kafka setup?