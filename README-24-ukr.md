# Branch 24 — Advanced Topics: Compression, AdminClient, Interceptors

## 1. Що вивчаємо у цій гілці

- **Compression** — gzip / lz4 / zstd / snappy: trade-off між CPU витратами та розміром.
- **AdminClient API** — програмне керування топіками та consumer groups без CLI.
- **ProducerInterceptor** — `onSend()`, `onAcknowledgement()`: хуки в pipeline producer.
- **ConsumerInterceptor** — `onConsume()`, `onCommit()`: хуки в pipeline consumer.
- **Benchmark** — порівняння всіх типів компресії за throughput та latency.
- **Quotas** — `producer_byte_rate` / `consumer_byte_rate`: обмеження per client.

---

## 2. Зміни порівняно з попередньою гілкою (branch23)

- **Видалено** SASL/SSL, ACL — повернуто до PLAINTEXT для простоти.
- **Додано** admin-service — новий мікросервіс для AdminClient REST API.
- **order-service** розширено: compression config, ProducerInterceptor,
  ConsumerInterceptor, benchmark endpoint.
- **Новий endpoint** `/api/benchmark/compare?count=N` — порівняння компресій.
- **Нові endpoints** у admin-service: topics CRUD, cluster info, consumer groups.

---

## 3. Архітектура

```
order-service (compression + interceptors)
  POST /api/orders ──[24.orders.created]──► notification (implicit)
  POST /api/benchmark/compare ──► порівняння типів компресії
  GET  /api/interceptor/stats ──► статистика interceptors

admin-service (AdminClient)
  GET  /api/admin/topics ──► list topics
  POST /api/admin/topics ──► create topic
  GET  /api/admin/cluster ──► cluster info
  ...
```

**Сервіси та порти (з docker-compose-24.yml):**
- `kafka` — 9092 (KRaft single-node)
- `kafka-ui` — 8080 → http://localhost:8080
- `order-service-b24` — 8081 → http://localhost:8081
- `admin-service-b24` — 8089 → http://localhost:8089

---

## 4. Ключові концепції

### Типи компресії

```
Тип       Рівень стиснення  CPU          Latency    Рекомендація
none      0%                мінімальний  мінімальна  dev, малі повідомлення
gzip      60–70%            високий      висока      batch, архівація
snappy    40–50%            середній     середня     Google (legacy)
lz4       40–50%            низький      низька      production (рекомендовано)
zstd      50–60%            середній     середня     production (best ratio, Kafka 2.1+)
```

```yaml
# application.yml
spring:
  kafka:
    producer:
      compression-type: lz4  # або: gzip, snappy, zstd, none
```

### Compression на рівні broker

```
compression.type=producer (default):
  Broker зберігає у форматі producer → no recompression, efficient

compression.type=gzip (broker overrides):
  Broker перекомпресовує незалежно від producer
  → double compression, не рекомендується

compression.type=uncompressed:
  Broker завжди розпаковує → теж не рекомендується
```

### AdminClient API

```kotlin
val adminClient = AdminClient.create(mapOf(
    AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers
))

// Список топіків
adminClient.listTopics().listings().get()

// Створити топік
adminClient.createTopics(listOf(NewTopic("my-topic", 3, 1))).all().get()

// Описати топік (партиції, ISR, лідер)
adminClient.describeTopics(listOf("my-topic")).allTopicNames().get()

// Змінити конфігурацію
val resource = ConfigResource(ConfigResource.Type.TOPIC, "my-topic")
val op = AlterConfigOp(ConfigEntry("retention.ms", "86400000"), AlterConfigOp.OpType.SET)
adminClient.incrementalAlterConfigs(mapOf(resource to listOf(op))).all().get()

// Consumer groups
adminClient.listConsumerGroups().all().get()
adminClient.describeConsumerGroups(listOf("my-group")).all().get()
```

### ProducerInterceptor

```kotlin
class MetricsProducerInterceptor : ProducerInterceptor<String, Any> {
    private val sentCount = AtomicLong(0)
    private val ackCount = AtomicLong(0)
    private val errorCount = AtomicLong(0)

    // ДО серіалізації та відправки — може змінити record
    override fun onSend(record: ProducerRecord<String, Any>): ProducerRecord<String, Any> {
        sentCount.incrementAndGet()
        return record  // ОБОВ'ЯЗКОВО повернути record
    }

    // ПІСЛЯ отримання ACK від брокера (або помилки)
    override fun onAcknowledgement(metadata: RecordMetadata?, exception: Exception?) {
        if (exception == null) ackCount.incrementAndGet()
        else errorCount.incrementAndGet()
    }

    override fun close() {}
    override fun configure(configs: Map<String, *>) {}
}
```

### HeaderEnrichmentInterceptor

```kotlin
class HeaderEnrichmentInterceptor : ProducerInterceptor<String, Any> {
    override fun onSend(record: ProducerRecord<String, Any>): ProducerRecord<String, Any> {
        record.headers()
            .add("x-service-name", "order-service".toByteArray())
            .add("x-sent-at", System.currentTimeMillis().toString().toByteArray())
            .add("x-trace-id", UUID.randomUUID().toString().toByteArray())
        return record
    }
}
```

**Конфігурація кількох interceptors:**
```yaml
spring:
  kafka:
    producer:
      properties:
        interceptor.classes: >
          com.kafkalab.order.interceptor.MetricsProducerInterceptor,
          com.kafkalab.order.interceptor.HeaderEnrichmentInterceptor
```

### ConsumerInterceptor

```kotlin
class TracingConsumerInterceptor : ConsumerInterceptor<String, Any> {
    // ДО обробки listener — може фільтрувати/збагачувати records
    override fun onConsume(records: ConsumerRecords<String, Any>): ConsumerRecords<String, Any> {
        for (record in records) {
            val sentAt = record.headers().lastHeader("x-sent-at")
                ?.let { String(it.value()) }
            log.info("[INTERCEPTOR] topic={} offset={} sentAt={}",
                record.topic(), record.offset(), sentAt)
        }
        return records  // можна повернути відфільтрований підмножина
    }

    // ПІСЛЯ commit offset
    override fun onCommit(offsets: Map<TopicPartition, OffsetAndMetadata>) {
        log.debug("[COMMIT] offsets={}", offsets)
    }
}
```

---

## 5. Як запустити

```bash
docker compose -f docker-compose-24.yml up --build

# Kafka UI:      http://localhost:8080
# order-service: http://localhost:8081
# admin-service: http://localhost:8089
```

---

## 6. Як тестувати

### A. Compression Benchmark

```bash
# Порівняти всі типи компресії (100 повідомлень по 1KB)
curl -s -X POST "http://localhost:8081/api/benchmark/compare?count=100" | jq .
# {
#   "none":  {"throughputMsgPerSec": 5000, "avgLatencyMs": 0.2},
#   "lz4":   {"throughputMsgPerSec": 4500, "avgLatencyMs": 0.22},
#   "gzip":  {"throughputMsgPerSec": 1000, "avgLatencyMs": 1.0},
#   "zstd":  {"throughputMsgPerSec": 2000, "avgLatencyMs": 0.5},
#   "snappy":{"throughputMsgPerSec": 3500, "avgLatencyMs": 0.28}
# }
```

### B. AdminClient API

```bash
# Список топіків
curl http://localhost:8089/api/admin/topics | jq .

# Кластер (брокери, controller)
curl http://localhost:8089/api/admin/cluster | jq .

# Створити топік
curl -X POST http://localhost:8089/api/admin/topics \
  -H "Content-Type: application/json" \
  -d '{"name":"my-new-topic","partitions":3,"replicationFactor":1}' | jq .

# Описати топік
curl http://localhost:8089/api/admin/topics/24.orders.created | jq .

# Конфігурація топіку (non-default значення)
curl http://localhost:8089/api/admin/topics/24.orders.created/config | jq .

# Змінити retention
curl -X PATCH http://localhost:8089/api/admin/topics/24.orders.created/config \
  -H "Content-Type: application/json" \
  -d '{"key":"retention.ms","value":"86400000"}' | jq .

# Consumer groups
curl http://localhost:8089/api/admin/consumer-groups | jq .
curl http://localhost:8089/api/admin/consumer-groups/notification-service-group | jq .

# Видалити топік
curl -X DELETE http://localhost:8089/api/admin/topics/my-new-topic | jq .
```

### C. Interceptor Stats

```bash
# Відправити кілька замовлень
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-1","itemCount":2}' | jq .

# Переглянути статистику interceptor
curl http://localhost:8081/api/interceptor/stats | jq .
# {"sent": 1, "acked": 1, "errors": 0}

# Kafka UI → topic 24.orders.created → Message → Headers
# x-service-name: order-service
# x-sent-at: 1234567890123
# x-trace-id: uuid-here
```

### D. Quotas (бонус)

```bash
# Обмежити producer до 1MB/s
docker exec kafka kafka-configs \
  --bootstrap-server localhost:9092 \
  --alter \
  --add-config 'producer_byte_rate=1048576' \
  --entity-type clients \
  --entity-name order-service

# Перевірити
docker exec kafka kafka-configs \
  --bootstrap-server localhost:9092 \
  --describe \
  --entity-type clients \
  --entity-name order-service
```

---

## 7. Поглиблений розгляд

### Коли спрацьовує onSend()

```
Producer pipeline:
  1. kafkaTemplate.send(record)
  2. ProducerInterceptor.onSend(record) ← ДО серіалізації!
      → можна змінити ключ, значення, headers, topic
      → ОБОВ'ЯЗКОВО повернути record (навіть незмінений)
  3. Серіалізація (key/value serializer)
  4. Partitioner
  5. Buffer (RecordAccumulator)
  6. Sender thread → broker

  ProducerInterceptor.onAcknowledgement(metadata, exception) ← ПІСЛЯ ACK
```

### Compression та batch

```
Компресія відбувається на рівні batch (не окремого record):
  linger.ms=20 → producer чекає 20мс щоб накопичити batch
  → більший batch → краща компресія

Для максимальної ефективності компресії:
  compression-type: lz4 (або zstd)
  linger.ms: 20–50ms
  batch.size: 65536 (64KB)
```

### AdminClient vs kafka-topics CLI

```
kafka-topics CLI:
  Shell script, зупиняється після виконання
  Підходить: одноразові операції, CI/CD pipelines

AdminClient API:
  Java/Kotlin код, тривалий lifecycle
  Підходить: динамічне керування топіками у runtime,
             self-service платформи, операційні дашборди,
             автоматичне provisioning

AdminClient.close() ОБОВ'ЯЗКОВО (закриває TCP connections)
```

---

## 8. Структура проекту

```
branch24_advanced_topics/
├── order-service/
│   ├── controller/
│   │   ├── OrderController.kt          ← POST /api/orders
│   │   ├── BenchmarkController.kt      ← POST /api/benchmark/compare?count=N
│   │   └── InterceptorController.kt    ← GET /api/interceptor/stats
│   ├── interceptor/
│   │   ├── MetricsProducerInterceptor.kt ← sentCount, ackCount, errorCount
│   │   ├── HeaderEnrichmentInterceptor.kt ← x-service-name, x-sent-at, x-trace-id
│   │   └── TracingConsumerInterceptor.kt  ← log traceId on consume
│   └── service/BenchmarkService.kt     ← порівняння 5 типів компресії
└── admin-service/
    ├── controller/AdminController.kt   ← всі /api/admin/* endpoints
    └── service/AdminClientService.kt   ← AdminClient operations
```

---

## 9. Що далі

- Ви пройшли всі 24 гілки Kafka Laboratory!
- Наступні теми для self-study:
  - Kafka Mirror Maker 2 (cross-cluster replication)
  - Tiered Storage (S3-backed log)
  - Kafka на Kubernetes (Strimzi operator)
  - Performance tuning (JVM, GC, OS tuning)

---

## 10. Слайди для лекції

### Слайд 1 — Compression Trade-offs

```
         Розмір    CPU     Latency
none:    ████████  ░░      ░░░░░░
lz4:     ████░░░   ░░░     ░░░░░░░
snappy:  ████░░░   ░░░░    ░░░░░░░░
zstd:    ███░░░░   ░░░░░   ░░░░░░░░░
gzip:    ██░░░░░   ░░░░░░  ░░░░░░░░░░

Рекомендація production: lz4 або zstd
```

### Слайд 2 — Interceptor Pipeline

```
send(record)
    │
    ▼
[onSend interceptor 1]  ← може змінити record
    │
    ▼
[onSend interceptor 2]  ← може змінити record
    │
    ▼
[Serializer]
    │
    ▼
[Partitioner]
    │
    ▼
[Buffer → Broker]
    │
    ▼
[onAcknowledgement]     ← ПІСЛЯ отримання ACK
```

### Слайд 3 — AdminClient use cases

```
kafka-topics CLI:            AdminClient API:
  Одноразово                   Runtime (у коді)
  ─────────────                ─────────────────────
  CI/CD pipeline               Self-service platform
  Manual ops                   Auto-provisioning
  Dev environment              Multi-tenant management
                               Topic health monitoring
```

---

## 11. Демонстраційний сценарій

```bash
#!/bin/bash
echo "=== Branch 24: Advanced Topics Demo ==="

docker compose -f docker-compose-24.yml up --build -d
sleep 60

echo ""
echo "=== A. Compression Benchmark ==="
curl -s -X POST "http://localhost:8081/api/benchmark/compare?count=50" | jq .

echo ""
echo "=== B. AdminClient: список топіків ==="
curl -s http://localhost:8089/api/admin/topics | jq '.[].name'

echo ""
echo "=== B. AdminClient: кластер ==="
curl -s http://localhost:8089/api/admin/cluster | jq .

echo ""
echo "=== B. AdminClient: створити топік ==="
curl -s -X POST http://localhost:8089/api/admin/topics \
  -H "Content-Type: application/json" \
  -d '{"name":"demo-topic","partitions":3,"replicationFactor":1}' | jq .

echo ""
echo "=== C. Interceptor: відправка та stats ==="
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-1","itemCount":3}' | jq .
sleep 2
curl -s http://localhost:8081/api/interceptor/stats | jq .

echo ""
echo "=== C. Kafka UI: перевірити headers ==="
echo "Відкрити: http://localhost:8080"
echo "Topics → 24.orders.created → останнє повідомлення → Headers"
echo "Очікуємо: x-service-name, x-sent-at, x-trace-id"

echo ""
echo "=== D. Quotas (бонус) ==="
docker exec kafka kafka-configs \
  --bootstrap-server localhost:9092 \
  --alter \
  --add-config 'producer_byte_rate=1048576' \
  --entity-type clients \
  --entity-name order-service
echo "Quota встановлено: 1MB/s"
```

---

## 12. Питання для самоперевірки

- Який тип компресії найкращий для production і чому?
- Де відбувається компресія — на рівні record чи batch?
- Чому `compression.type=producer` на брокері є рекомендованим?
- Коли `onSend()` interceptor виконується: до чи після серіалізації?
- Чим відрізняється ProducerInterceptor від ConsumerInterceptor?
- Що відбудеться якщо `onSend()` поверне null?
- Яка різниця між AdminClient API та kafka-topics CLI?
- Навіщо потрібен `AdminClient.close()`?