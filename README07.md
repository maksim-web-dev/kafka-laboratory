# Branch 07 — Producer Configuration

## Що змінилося порівняно з branch06

| Аспект | branch06 | branch07 |
|--------|----------|----------|
| Default producer acks | не задано (=1) | **`acks=all`** |
| Idempotent producer | — | **`enable.idempotence=true`** |
| Benchmark | — | **`POST /api/benchmark/run?mode=acks0\|acks1\|acks-all`** |
| Порівняння режимів | — | **`POST /api/benchmark/compare?count=100`** |
| Нові конфіги | — | `linger.ms`, `batch.size`, `retries`, `delivery.timeout.ms` |
| Порти | 9102/8104/8105/8106 | **9104/8108/8109/8110** |

---

## Ключові концепції

### acks — підтвердження запису

| Значення | Хто підтверджує | Втрата можлива? | Latency |
|----------|-----------------|-----------------|---------|
| `acks=0` | ніхто — fire-and-forget | **так, при будь-якому збої** | найменша |
| `acks=1` | лідер партиції | **так, якщо лідер впаде до реплікації** | середня |
| `acks=all` | всі ISR (in-sync replicas) | **ні** (при `min.insync.replicas` виконано) | найбільша |

```
Producer → Kafka Broker (leader)
              │
              ├── acks=0: нічого не чекаємо, повертаємо одразу
              ├── acks=1: чекаємо "OK" від leader
              └── acks=all: чекаємо "OK" від leader + replica-1 + replica-2
```

### min.insync.replicas

З одним брокером `min.insync.replicas=1` (default). Якщо б було 3 брокери:

```
topic: orders.created, replication-factor=3, min.insync.replicas=2

Producer acks=all → Kafka перевіряє: >= 2 ISR підтвердили?
  ISR: [broker-1(leader), broker-2, broker-3] → OK
  ISR після збою broker-2: [broker-1, broker-3] → OK (2 >= 2)
  ISR після збою broker-2 + broker-3: [broker-1] → NotEnoughReplicasException ✗
```

### Idempotent Producer

```
enable.idempotence=true вимагає:
  - acks=all
  - max.in.flight.requests.per.connection <= 5
  - retries >= 1

Механізм:
  Producer отримує sequence number (producer-id + sequence per partition)
  Kafka broker відхиляє дублікати з тим самим sequence number
  
Без idempotence (acks=1, retries=3):
  Send seq=5 → timeout (отримано, але підтвердження не прийшло)
  Retry seq=5 → broker записує дублікат ← ПРОБЛЕМА

З idempotence:
  Send seq=5 → timeout
  Retry seq=5 → broker: "вже маємо seq=5, пропускаємо" → no duplicate ✓
```

### linger.ms + batch.size — батчинг

```
linger.ms=0 (default): відправляти одразу, batch може бути з 1 повідомлення
linger.ms=20:          чекати 20мс для накопичення більшого batch

Producer buffer:
  t=0ms: msg-1 → acks=0, linger=0 → одразу відправляємо batch[msg-1]
  
  t=0ms:  msg-1 → acks=all, linger=20ms → чекаємо
  t=8ms:  msg-2 → додаємо до batch
  t=15ms: msg-3 → додаємо до batch
  t=20ms: → відправляємо batch[msg-1, msg-2, msg-3] → 1 мережевий запит замість 3
```

---

## Три конфігурації Producer

### acks=0: максимальна швидкість

```yaml
acks: "0"
linger.ms: 0
batch.size: 16384   # 16 KB
retries: 0          # немає сенсу ретраїти fire-and-forget
```

Використовувати для: метрики, логи, де втрата окремих подій прийнятна.

### acks=1: баланс

```yaml
acks: "1"
linger.ms: 5
batch.size: 32768   # 32 KB
retries: 3
retry.backoff.ms: 100
delivery.timeout.ms: 30000
```

Використовувати для: внутрішні події, де дублікати допустимі.

### acks=all + idempotent: максимальна надійність

```yaml
acks: "all"
linger.ms: 20        # накопичувати batch для вищого throughput
batch.size: 65536    # 64 KB
enable.idempotence: true
max.in.flight.requests.per.connection: 5
delivery.timeout.ms: 30000
```

Використовувати для: критичні бізнес-події (замовлення, платежі).

---

## Як запустити

```bash
# Зупинити branch06
docker stop kafka kafka-ui order-service-b06 notification-service-b06 2>/dev/null || true

# Запустити branch07
docker compose -f docker-compose-07.yml down -v 2>/dev/null || true
docker compose -f docker-compose-07.yml up --build
```

Перевірити 4 контейнери:

```bash
docker compose -f docker-compose-07.yml ps
# kafka, kafka-ui, order-service-b07, notification-service-b07
```

---

## Як протестувати

### 1. Запустити benchmark для одного режиму

```bash
# acks=0: fire-and-forget
curl -s -X POST "http://localhost:8109/api/benchmark/run?mode=acks0&count=200" | jq

# acks=1: leader ack
curl -s -X POST "http://localhost:8109/api/benchmark/run?mode=acks1&count=200" | jq

# acks=all + idempotent
curl -s -X POST "http://localhost:8109/api/benchmark/run?mode=acks-all&count=200" | jq
```

Очікуваний результат (приклад):

```json
{
  "mode": "acks0",
  "acksConfig": "0",
  "idempotent": false,
  "lingerMs": 0,
  "batchSizeBytes": 16384,
  "messageCount": 200,
  "totalDurationMs": 45,
  "throughputMsgPerSec": 4444.4,
  "avgLatencyMs": 0.22
}
```

### 2. Порівняти всі три режими

```bash
curl -s -X POST "http://localhost:8109/api/benchmark/compare?count=200" | jq '
  to_entries | map({
    mode: .key,
    throughput: .value.throughputMsgPerSec,
    avgLatency: .value.avgLatencyMs,
    acks: .value.acksConfig
  })'
```

Очікувана таблиця:

```
mode      | throughput (msg/s) | avgLatency (ms)
----------|--------------------|----------------
acks0     | ~5000              | ~0.2
acks1     | ~1000              | ~1.0
acks-all  | ~800               | ~1.2  (але з батчингом linger=20ms)
```

> З одним брокером різниця між acks=1 та acks=all мінімальна, оскільки є лише одна ISR.
> В production (3 брокери, replication-factor=3, min.insync.replicas=2) acks=all помітно повільніший.

### 3. Перевірити idempotent producer

Idempotent producer видно у логах Kafka при реєстрації:

```bash
docker logs kafka 2>&1 | grep -i "producer"
# [ProducerStateManager] Loading producer state from offset...
```

Також у Kafka UI: Brokers → Configs → поискати `min.insync.replicas`.

### 4. Звичайні замовлення (через production-safe default producer)

```bash
# Default producer: acks=all, idempotent=true
curl -s -X POST http://localhost:8109/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-01","product":"Book","quantity":1,"totalAmount":25.00}' | jq

# Лог order-service покаже:
# OrderCreated published → topic=orders.created, partition=X, offset=Y, key=user-01
```

### 5. Kafka UI — переглянути producer метрики

Відкрити: [http://localhost:8108](http://localhost:8108)

- `Topics` → `orders.created` → бачимо повідомлення від benchmark
- `Brokers` → `Metrics` → шукати `record-send-rate`, `record-error-rate`

---

## Параметри producer і їх вплив

| Параметр | Default | Вплив на |
|----------|---------|----------|
| `acks` | `1` | надійність vs latency |
| `retries` | `2147483647` | at-least-once при мережевих помилках |
| `retry.backoff.ms` | `100` | пауза між retry |
| `delivery.timeout.ms` | `120000` | максимальний час одного send() |
| `linger.ms` | `0` | batch accumulation time → throughput |
| `batch.size` | `16384` | максимальний розмір batch у байтах |
| `enable.idempotence` | `false` | no-duplicate при retry |
| `max.in.flight.requests` | `5` | паралельні запити (≤5 з idempotence) |
| `compression.type` | `none` | gzip/snappy/lz4 → розмір vs CPU |

---

## Структура змін відносно branch06

```
kafka-laboratory/
├── docker-compose-07.yml                              ← нові порти b07 (9104/8108/8109/8110)
├── branch07_producer_configuration/
│   ├── order-service/
│   │   └── src/main/resources/application.yml        ← acks=all, enable.idempotence, delivery.timeout
│   │   └── src/main/kotlin/.../service/
│   │       └── OrderService.kt                       ← стандартна публікація замовлень
│   │       └── BenchmarkService.kt                   ← NEW: runBenchmark(), compareAll()
│   │   └── src/main/kotlin/.../config/
│   │       └── KafkaTopicConfig.kt                   ← топіки 07.orders.*, 07.benchmark
│   │   └── src/main/kotlin/.../model/
│   │       └── BenchmarkResult.kt                    ← NEW: data class
│   │   └── src/main/kotlin/.../controller/
│   │       └── BenchmarkController.kt                ← NEW: /api/benchmark/run, /api/benchmark/compare
│   └── notification-service/                         ← auto-commit (без manual commit)
└── README07.md
```

---

## Ключові концепції цієї гілки

| Концепція | Що демонструє |
|-----------|---------------|
| **acks=0** | Fire-and-forget, max throughput, можлива втрата |
| **acks=1** | Leader ack, баланс швидкості та надійності |
| **acks=all** | All-ISR ack, максимальна надійність |
| **Idempotent producer** | `enable.idempotence=true` → no duplicates при retry |
| **linger.ms + batch.size** | Батчинг для збільшення throughput |
| **delivery.timeout.ms** | Максимальний час спроб доставки |
| **Producer Callback** | `.whenComplete { result, ex -> }` для async обробки помилок |

---

## Що далі — branch08

У наступній гілці вивчаємо **Consumer Configuration**:
- Incremental Cooperative Rebalance vs EAGER
- Static Group Membership (`group.instance.id`)
- `max.poll.records`, `max.poll.interval.ms`
- `session.timeout.ms`, `heartbeat.interval.ms`
- Graceful shutdown