# Branch 24 — Advanced Topics: Compression, AdminClient, Interceptors

## Що вивчаємо

| Концепція | Деталі |
|-----------|--------|
| **Compression** | gzip / lz4 / zstd / snappy / none — trade-off CPU vs розмір |
| **AdminClient API** | Програмне керування топіками і consumer groups |
| **ProducerInterceptor** | `onSend()`, `onAcknowledgement()` — hooks в pipeline producer |
| **ConsumerInterceptor** | `onConsume()`, `onCommit()` — hooks в pipeline consumer |

## Порти

| Сервіс | Порт |
|--------|------|
| Kafka (external) | 9129 |
| Kafka UI | 8209 |
| order-service (compression + interceptors) | 8210 |
| admin-service (AdminClient API) | 8211 |

## Запуск

```bash
docker compose -f docker-compose-24.yml up --build
```

---

## A. Compression

### Типи та порівняння

| Тип | Рівень стиснення | CPU | Latency | Use case |
|-----|-----------------|-----|---------|----------|
| `none` | 0% | мінімальний | мінімальна | Dev, малі повідомлення |
| `gzip` | ~60–70% | високий | висока | Batch, архівація |
| `snappy` | ~40–50% | середній | середня | Баланс (Google) |
| `lz4` | ~40–50% | низький | низька | Production (рекомендовано) |
| `zstd` | ~50–60% | середній | середня | Production (best ratio, Kafka 2.1+) |

```yaml
# application.yml — вибір типу компресії
spring:
  kafka:
    producer:
      compression-type: lz4  # або: gzip, snappy, zstd, none
```

### Broker-side config

```
compression.type=producer (broker default):
  Broker зберігає повідомлення у форматі producer-а (без перекомпресії)
  
compression.type=gzip (broker overrides):
  Broker перекомпресовує незалежно від producer — не рекомендується (double compression)
  
compression.type=uncompressed:
  Broker завжди розпаковує — також не рекомендується
```

### Benchmark

```bash
# Порівняти всі типи компресії (100 повідомлень по 1KB)
curl -s -X POST "http://localhost:8210/api/benchmark/compare?count=100" | jq

# Очікуваний результат:
# {
#   "none":  {"throughputMsgPerSec": 5000, "avgLatencyMs": 0.2},
#   "lz4":   {"throughputMsgPerSec": 4500, "avgLatencyMs": 0.22},
#   "gzip":  {"throughputMsgPerSec": 1000, "avgLatencyMs": 1.0},
#   "zstd":  {"throughputMsgPerSec": 2000, "avgLatencyMs": 0.5},
#   "snappy":{"throughputMsgPerSec": 3500, "avgLatencyMs": 0.28}
# }
```

---

## B. AdminClient API

AdminClient дозволяє **програмно** керувати кластером — те саме що kafka-topics CLI, але з Java/Kotlin.

### Основні операції

```kotlin
val adminClient = AdminClient.create(mapOf(
    AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9129"
))

// Створити топік
adminClient.createTopics(listOf(NewTopic("my-topic", 3, 1))).all().get()

// Отримати список топіків
adminClient.listTopics().listings().get()

// Описати топік (партиції, ISR, лідер)
adminClient.describeTopics(listOf("my-topic")).allTopicNames().get()

// Змінити конфігурацію топіку
val resource = ConfigResource(ConfigResource.Type.TOPIC, "my-topic")
val op = AlterConfigOp(ConfigEntry("retention.ms", "86400000"), AlterConfigOp.OpType.SET)
adminClient.incrementalAlterConfigs(mapOf(resource to listOf(op))).all().get()

// Consumer groups
adminClient.listConsumerGroups().all().get()
adminClient.describeConsumerGroups(listOf("my-group")).all().get()
```

### REST API admin-service

```bash
# Список топіків
curl http://localhost:8211/api/admin/topics | jq

# Описати кластер (брокери, controller)
curl http://localhost:8211/api/admin/cluster | jq

# Створити топік
curl -X POST http://localhost:8211/api/admin/topics \
  -H "Content-Type: application/json" \
  -d '{"name":"my-new-topic","partitions":3,"replicationFactor":1}' | jq

# Описати топік
curl http://localhost:8211/api/admin/topics/24.orders.created | jq

# Переглянути конфігурацію топіку (тільки non-default значення)
curl http://localhost:8211/api/admin/topics/24.orders.created/config | jq

# Змінити retention топіку
curl -X PATCH http://localhost:8211/api/admin/topics/24.orders.created/config \
  -H "Content-Type: application/json" \
  -d '{"key":"retention.ms","value":"86400000"}' | jq

# Consumer groups
curl http://localhost:8211/api/admin/consumer-groups | jq
curl http://localhost:8211/api/admin/consumer-groups/notification-service-group | jq

# Видалити топік
curl -X DELETE http://localhost:8211/api/admin/topics/my-new-topic | jq
```

---

## C. Producer Interceptors

Interceptor — це хук що виконується у pipeline producer/consumer **до** або **після** ключових операцій.

```
ProducerInterceptor<K,V>:
  onSend(ProducerRecord) → ProducerRecord   ← ДО серіалізації і відправки
  onAcknowledgement(RecordMetadata, Exception) ← ПІСЛЯ отримання ACK від брокера
  close() / configure()
```

### MetricsProducerInterceptor

Відстежує кількість відправлених/підтверджених/помилкових повідомлень:

```kotlin
class MetricsProducerInterceptor : ProducerInterceptor<String, Any> {
    private val sentCount = AtomicLong(0)
    private val ackCount = AtomicLong(0)
    
    override fun onSend(record: ProducerRecord<String, Any>): ProducerRecord<String, Any> {
        sentCount.incrementAndGet()
        return record  // ОБОВ'ЯЗКОВО повернути record (можна змінений)
    }
    
    override fun onAcknowledgement(metadata: RecordMetadata?, exception: Exception?) {
        if (exception == null) ackCount.incrementAndGet()
    }
}
```

### HeaderEnrichmentInterceptor

Додає tracing headers до кожного повідомлення:

```kotlin
override fun onSend(record: ProducerRecord<String, Any>): ProducerRecord<String, Any> {
    record.headers()
        .add("x-service-name", "order-service".toByteArray())
        .add("x-sent-at", System.currentTimeMillis().toString().toByteArray())
    return record
}
```

### Конфігурація (кілька interceptors через кому)

```yaml
spring:
  kafka:
    producer:
      properties:
        interceptor.classes: >
          com.kafkalab.order.interceptor.MetricsProducerInterceptor,
          com.kafkalab.order.interceptor.HeaderEnrichmentInterceptor
```

### Перевірити stats

```bash
# Перегляд статистики interceptor
curl http://localhost:8210/api/interceptor/stats | jq
# {"sent": 5, "acked": 5, "errors": 0}

# Перегляд headers у Kafka UI — кожне повідомлення матиме x-service-name, x-sent-at
```

---

## D. Consumer Interceptors

```
ConsumerInterceptor<K,V>:
  onConsume(ConsumerRecords<K,V>) → ConsumerRecords<K,V>   ← ДО обробки listener
  onCommit(Map<TopicPartition, OffsetAndMetadata>)          ← ПІСЛЯ commit
  close() / configure()
```

```kotlin
class TracingConsumerInterceptor : ConsumerInterceptor<String, Any> {
    override fun onConsume(records: ConsumerRecords<String, Any>): ConsumerRecords<String, Any> {
        for (record in records) {
            val traceId = record.headers().lastHeader("x-sent-at")?.let {
                String(it.value())
            }
            log.info("[CONSUMER-INTERCEPTOR] topic={} offset={} traceId={}", 
                record.topic(), record.offset(), traceId)
        }
        return records  // можна повернути відфільтрований список
    }
}
```

---

## E. Quotas (бонус)

```bash
# Обмежити producer до 1MB/s через CLI
docker exec kafka-b24 kafka-configs \
  --bootstrap-server localhost:9092 \
  --alter \
  --add-config 'producer_byte_rate=1048576' \
  --entity-type clients \
  --entity-name order-service

# Перевірити
docker exec kafka-b24 kafka-configs \
  --bootstrap-server localhost:9092 \
  --describe \
  --entity-type clients \
  --entity-name order-service
```

---

## Ключові концепції

| Концепція | Що демонструє |
|-----------|---------------|
| **lz4 compression** | Найкращий баланс CPU/розмір для production |
| **`compression.type=producer`** | Broker зберігає у форматі producer без перекомпресії |
| **AdminClient** | Програмне керування топіками без CLI |
| **`ProducerInterceptor.onSend()`** | Виконується ДО серіалізації — може змінювати record |
| **`ProducerInterceptor.onAcknowledgement()`** | Виконується ПІСЛЯ ACK від брокера |
| **`interceptor.classes`** | Кілька interceptors через кому — виконуються по черзі |
| **Quotas** | `producer_byte_rate` / `consumer_byte_rate` — обмеження per client |