# Branch 14 — Kafka Streams

## Що вивчаємо

| Концепція | Деталі |
|-----------|--------|
| **KStream** | Необмежений потік подій, обробка record-by-record |
| **KTable** | Changelog stream — зберігає останній стан per key (аналог таблиці) |
| **groupBy / count** | Перегруповка потоку та агрегація |
| **Windowed aggregations** | Tumbling window (фіксований розмір, без overlap) |
| **State stores** | RocksDB-backed сховища стану, доступні через REST Interactive Queries |
| **`@EnableKafkaStreams`** | Spring Kafka інтеграція для Kafka Streams |

## Архітектура

```
order-service ──[14.orders.created]──► analytics-service (Kafka Streams)
                                              │
                    ┌─────────────────────────┼──────────────────────────────┐
                    │                         │                              │
              [count by userId]         [1-min tumbling             [filter high-value]
              KGroupedStream             window by category]        totalAmount >= 50
              .count()                  .windowedBy()                      │
              KTable                    .count()                            │
              state store                   │                               │
                    │                    peek/log                           ▼
                    ▼                                          [14.analytics.orders-high-value]
        [14.analytics.orders-by-user]
        (Long: userId → count)
```

## Kafka Streams топологія (analytics-service)

### 1. Rolling count per userId → KTable + state store

```
orders.created
  .groupBy(userId)     ← re-key stream by userId
  .count()             ← KTable<userId, Long>; backed by "orders-by-user-store"
  .toStream()
  .to("14.analytics.orders-by-user")
```

**KTable** зберігає лише актуальне значення per key. При кожному новому замовленні count збільшується і записується в state store.

### 2. Tumbling window (1 хвилина) per category

```
orders.created
  .groupBy(category)
  .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
  .count()             ← KTable<Windowed<category>, Long>
  .toStream()
  .peek(log)           ← виводить у логи кожне оновлення вікна
```

**Tumbling window** — вікна не перекриваються: `[0:00–1:00)`, `[1:00–2:00)`, тощо.

### 3. Tumbling window (5 хвилин) — сума продажів per userId

```
orders.created
  .groupBy(userId)
  .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
  .aggregate({ 0.0 }, { _, v, agg -> agg + v.totalAmount })
  .toStream()
  .map { key, value -> KeyValue(key.key(), value) }
  .peek(log)
```

### 4. Filter high-value orders

```
orders.created
  .filter { _, v -> v.totalAmount >= 50.0 }
  .to("14.analytics.orders-high-value")
```

## State Store (Interactive Queries)

Kafka Streams зберігає стан у локальний RocksDB store. Spring Kafka дає доступ через `StreamsBuilderFactoryBean.kafkaStreams`.

```kotlin
val store = streams.store(
    StoreQueryParameters.fromNameAndType("orders-by-user-store", QueryableStoreTypes.keyValueStore())
)
store.all().forEach { println("${it.key} -> ${it.value}") }
```

## Порти

| Сервіс | Порт |
|--------|------|
| Kafka (external) | 9115 |
| Schema Registry | 8141 |
| Kafka UI | 8140 |
| order-service | 8142 |
| analytics-service | 8143 |

## Запуск

```bash
docker compose -f docker-compose-14.yml up --build
```

Kafka UI: http://localhost:8140

## API

### Створити одне замовлення
```bash
curl -s -X POST http://localhost:8142/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-1", "itemCount": 3, "category": "ELECTRONICS"}' | jq .
```

### Пакетна генерація (5 замовлень для кількох users)
```bash
curl -s -X POST "http://localhost:8142/api/orders/batch?users=user-1,user-2,user-3&count=10"
```

### Переглянути кількість замовлень per userId (state store query)
```bash
curl -s http://localhost:8143/api/analytics/orders-by-user | jq .
```

### Топ-3 userId за кількістю замовлень
```bash
curl -s "http://localhost:8143/api/analytics/top-users?limit=3" | jq .
```

## Демо сценарій

```bash
# 1. Генеруємо 20 замовлень для 3 users
curl -s -X POST "http://localhost:8142/api/orders/batch?users=alice,bob,charlie&count=20"

# 2. Дивимось state store — хто скільки замовив
curl -s http://localhost:8143/api/analytics/orders-by-user | jq .
# {"alice": 7, "bob": 8, "charlie": 5}  (приблизно)

# 3. Топ-3 users
curl -s "http://localhost:8143/api/analytics/top-users?limit=3" | jq .

# 4. Дивимось у Kafka UI topics:
#    14.analytics.orders-by-user    — Long values (count)
#    14.analytics.orders-high-value — OrderCreatedEvent (totalAmount >= 50)
```

## KStream vs KTable

| | KStream | KTable |
|--|--|--|
| **Семантика** | Кожен запис — нова подія | Кожен запис — оновлення стану |
| **Аналогія** | Append-only лог | Поточний знімок per key |
| **Приклад** | `orders.created` | результат `count()` per userId |
| **Changelog** | Немає | Зберігається в Kafka topic |

## Типи вікон

| Тип | Опис | API |
|-----|------|-----|
| **Tumbling** | Фіксований розмір, без overlap | `TimeWindows.ofSizeWithNoGrace(Duration)` |
| **Hopping** | Фіксований розмір, з overlap | `TimeWindows.of(size).advanceBy(advance)` |
| **Session** | Динамічний розмір — gap між подіями | `SessionWindows.ofInactivityGapWithNoGrace(gap)` |
| **Sliding** | Overlap вікна для кожного запису | `SlidingWindows.ofTimeDifferenceWithNoGrace(timeDiff)` |

## Нові залежності (analytics-service)

```kotlin
implementation("org.apache.kafka:kafka-streams")
implementation("io.confluent:kafka-streams-avro-serde:7.7.1")
```

## Стек

- Spring Boot 3.3.4 + Spring Kafka + Kafka Streams
- Apache Avro 1.11.4
- Confluent `kafka-streams-avro-serde` 7.7.1
- Confluent Schema Registry 8.0.1
- Confluent Kafka 8.0.1 (KRaft mode)