# Branch 17 — Observability: Prometheus + Grafana + kafka-exporter

## 1. Що вивчаємо у цій гілці

- **Consumer Lag** — ключова метрика Kafka: наскільки consumer відстає від producer.
- **kafka-exporter** — sidecar-сервіс що збирає метрики з Kafka broker та перетворює у
  Prometheus формат.
- **Prometheus** — система збору та зберігання time-series метрик.
- **Grafana** — дашборди для візуалізації; provisioning через конфігураційні файли.
- **pause / resume / slow** API — симуляція різних станів consumer для спостереження lag.
- **`max.poll.records=1`** — чому маленьке значення корисне для демонстрації lag.

---

## 2. Зміни порівняно з попередньою гілкою (branch16)

- **Повернуто** single-broker (замість 3-broker cluster) — фокус на observability, не HA.
- **Додано** kafka-exporter, prometheus, grafana.
- **Новий endpoint** `/api/orders/flood?count=N` для швидкого генерування lag.
- **Новий API** notification-service: `/api/consumer/pause`, `/resume`, `/slow`, `/fast`.
- **`max.poll.records=1`** у notification-service — lag змінюється погобіцянково.
- **Grafana provisioning**: datasource + dashboard завантажуються автоматично.

---

## 3. Архітектура

```
order-service ──► Kafka (17.orders.created) ──► notification-service
                        │
                  kafka-exporter (/metrics)
                        │
                  Prometheus ──► Grafana (dashboard)
```

**Метрики flow:**
```
Kafka broker → kafka-exporter :9308 → Prometheus :9090 → Grafana :3000
```

**Сервіси та порти (з docker-compose-17.yml):**
- `kafka` — 9092 (KRaft single-node)
- `kafka-ui` — 8080 → http://localhost:8080
- `kafka-exporter-b17` — :9308 (Prometheus scrape endpoint, без external port)
- `prometheus-b17` — 9090 → http://localhost:9090
- `grafana-b17` — 3000 → http://localhost:3000 (admin/admin)
- `order-service-b17` — 8081 → http://localhost:8081
- `notification-service-b17` — 8082 → http://localhost:8082

---

## 4. Ключові концепції

### Consumer Lag

```
Consumer lag = Latest Offset − Consumer Offset

topic: 17.orders.created
  partition-0:
    latest offset:   100  ← producer записав 100 повідомлень
    consumer offset: 75   ← consumer обробив 75
    LAG:             25   ← 25 повідомлень очікують обробки

Lag > 0 означає: consumer не встигає за producer або тимчасово зупинений.
Lag → 0: consumer обробив всі повідомлення.

kafka_consumergroup_lag{topic="17.orders.created", partition="0"} 25
```

### kafka-exporter метрики

```
kafka_consumergroup_lag            ← відставання per partition
kafka_consumergroup_current_offset ← поточний offset consumer
kafka_topic_partition_current_offset ← останній offset в топіку
kafka_brokers                      ← кількість живих брокерів
kafka_topic_partitions             ← кількість партицій

Scrape interval: 15s (у prometheus.yml)
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

- `max.poll.records=1` — consumer бере по 1 повідомленню за poll.
  Lag збільшується/зменшується на 1 за раз → гарно видно на Grafana.
- `max.poll.interval.ms=600000` — якщо consumer "призупинений" (`pause` API), Kafka
  не виключає його з групи до 10 хвилин.

### Grafana Provisioning

```
grafana/provisioning/
├── datasources/
│   └── prometheus.yaml  ← автопідключення Prometheus як datasource
└── dashboards/
    └── dashboards.yaml  ← де шукати JSON дашборди

grafana/dashboards/
└── kafka-lag.json       ← 7 панелей (завантажується автоматично)
```

---

## 5. Як запустити

```bash
docker compose -f docker-compose-17.yml up --build

# Kafka UI:   http://localhost:8080
# Prometheus: http://localhost:9090
# Grafana:    http://localhost:3000  (admin/admin)
```

**Перевірка:**
```bash
# Переконатись що kafka-exporter збирає метрики
curl -s http://localhost:9090/api/v1/query \
  --data-urlencode 'query=kafka_brokers' | jq .data.result[0].value[1]
# "1"
```

---

## 6. Як тестувати

### Демо 1 — Базовий lag

```bash
# Призупинити consumer
curl -X POST http://localhost:8082/api/consumer/pause

# Надіслати 100 повідомлень
curl -X POST "http://localhost:8081/api/orders/flood?count=100"

# Grafana → "Kafka Observability" dashboard
# Consumer Lag: ~100
# Lag Over Time: зростаючий графік
```

### Демо 2 — Відновлення consumer (lag → 0)

```bash
curl -X POST http://localhost:8082/api/consumer/resume

# Grafana → lag поступово знижується до 0
```

### Демо 3 — Повільний consumer

```bash
# Обробка 1 повідомлення кожні 3 секунди
curl -X POST "http://localhost:8082/api/consumer/slow?ms=3000"

# Надіслати 30 повідомлень
curl -X POST "http://localhost:8081/api/orders/flood?count=30"

# Grafana → lag зростає (producer швидший за consumer)
# Після ~90 сек: consumer наздоганяє, lag → 0

# Повернути нормальну швидкість
curl -X POST http://localhost:8082/api/consumer/fast
```

### Демо 4 — Prometheus UI

```bash
# Відкрити: http://localhost:9090
# Graph → запит:
#   kafka_consumergroup_lag
#   kafka_consumergroup_lag{consumergroup="notification-service-group"}
#   sum(kafka_consumergroup_lag) by (consumergroup)
```

---

## 7. Поглиблений розгляд

### Grafana Dashboard — 7 панелей

```
1. Consumer Lag (total)       ← sum(kafka_consumergroup_lag) по всіх партиціях
2. Messages in Topic          ← kafka_topic_partition_current_offset
3. Consumer Offset            ← kafka_consumergroup_current_offset
4. Active Brokers             ← kafka_brokers
5. Consumer Lag Over Time     ← lag per partition у часі (line chart)
6. Producer vs Consumer Offset ← gap між ними = lag (dual-axis chart)
7. Production Rate            ← rate(kafka_topic_partition_current_offset[1m])
```

### Prometheus scrape конфігурація

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
    container.pause()    // Kafka consumer.pause(partitions) — перестає poll
}

fun resume() {
    container.resume()   // Kafka consumer.resume(partitions)
}

fun slow(ms: Long) {
    processingDelayMs = ms   // Thread.sleep(ms) перед ack
}
```

`container.pause()` не зупиняє JVM-поток, а instructs consumer не брати нові poll батчі.
Offset не комітується → lag накопичується.

---

## 8. Структура проекту

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

## 9. Що далі

- **Branch 18** — Security: SASL/PLAIN автентифікація та ACL (Access Control Lists).
- **Branch 19** — Production-like: monitoring + cluster + security + saga combined.

---

## 10. Слайди для лекції

### Слайд 1 — Consumer Lag

```
Producer:  ──●──●──●──●──●──●──●──●──●──  (offset 100)
Consumer:  ──●──●──●──●──●──●─────────    (offset 75)
                                    ↑↑↑
                                   LAG=25

Consumer lag = відставання у повідомленнях.
Критичний lag → затримка бізнес-логіки → алерти!
```

### Слайд 2 — Monitoring Stack

```
Kafka broker
     │
kafka-exporter ─► /metrics (Prometheus format)
     │
Prometheus ────── scrape every 15s → time-series DB
     │
Grafana ────────── PromQL queries → dashboards, alerts
```

### Слайд 3 — PromQL запити

```sql
-- Поточний lag
kafka_consumergroup_lag

-- Lag по group
kafka_consumergroup_lag{consumergroup="notification-service-group"}

-- Швидкість виробництва (повідомлень/сек)
rate(kafka_topic_partition_current_offset[1m])

-- Сумарний lag
sum(kafka_consumergroup_lag) by (consumergroup)
```

---

## 11. Демонстраційний сценарій

```bash
#!/bin/bash
echo "=== Branch 17: Observability Demo ==="

docker compose -f docker-compose-17.yml up --build -d
sleep 60

echo ""
echo "=== Перевірка метрик ==="
curl -s "http://localhost:9090/api/v1/query?query=kafka_brokers" | jq .data.result[0].value[1]

echo ""
echo "=== Крок 1: Призупинити consumer ==="
curl -X POST http://localhost:8082/api/consumer/pause

echo ""
echo "=== Крок 2: Flood 100 повідомлень ==="
curl -s -X POST "http://localhost:8081/api/orders/flood?count=100"
echo "Відкрити Grafana: http://localhost:3000"
echo "Dashboard: Kafka Observability"
echo "Lag повинен бути ~100"
sleep 20

echo ""
echo "=== Крок 3: Відновити consumer ==="
curl -X POST http://localhost:8082/api/consumer/resume
echo "Lag знижується..."
sleep 15

echo ""
echo "=== Крок 4: Повільний consumer + flood ==="
curl -X POST "http://localhost:8082/api/consumer/slow?ms=2000"
curl -s -X POST "http://localhost:8081/api/orders/flood?count=20"
echo "Lag зростає (consumer повільніший за producer)"
sleep 30
curl -X POST http://localhost:8082/api/consumer/fast
echo "Consumer наздогнав"
```

---

## 12. Питання для самоперевірки

- Що таке consumer lag і як він рахується?
- Яку роль виконує kafka-exporter у monitoring stack?
- Навіщо `max.poll.records=1` для демонстрації lag?
- Що відбувається з consumer group при `container.pause()` при `max.poll.interval.ms=600000`?
- Яка різниця між `consumer offset` та `topic partition current offset`?
- Чому PromQL `rate()` корисний для відстеження throughput?
- Що таке Grafana provisioning і чому це краще за ручне налаштування?
- Який алерт варто налаштувати першим у production Kafka?