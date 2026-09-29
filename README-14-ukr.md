# Branch 14 — Kafka Streams

## 1. Що вивчаємо у цій гілці

- **KStream** — необмежений потік подій, record-by-record обробка.
- **KTable** — changelog stream: зберігає лише останній стан per key (аналог матеріалізованого view).
- **groupBy / count** — перегруповка потоку та підрахунок подій.
- **Tumbling Window** — фіксований невідновлюваний часовий інтервал (без overlap).
- **State Store** — RocksDB-backed локальне сховище стану, доступне через **Interactive Queries**.
- **`@EnableKafkaStreams`** — Spring Kafka автоматично керує lifecycle Streams-застосунку.

---

## 2. Зміни порівняно з попередньою гілкою (branch13)

- **Замінено** notification-service на analytics-service з **Kafka Streams** обробкою.
- **Видалено** inventory-service та компенсуючі топіки саги.
- **Нові топіки**: `14.analytics.orders-by-user` (KTable результати) та
  `14.analytics.orders-high-value` (фільтровані події).
- **Нові залежності**: `kafka-streams`, `kafka-streams-avro-serde`.
- **order-service** додав поля `category` та `totalAmount` до OrderCreatedEvent.

---

## 3. Архітектура

```
order-service ──[14.orders.created]──► analytics-service (Kafka Streams)
                                               │
                   ┌───────────────────────────┼──────────────────────────────┐
                   │                           │                              │
             [count by userId]          [1-хв вікно                   [filter >= $50]
             groupBy(userId)             per category]                        │
             .count()                   .windowedBy()                         │
             KTable                     .count()                              ▼
             state store                peek + log            [14.analytics.orders-high-value]
                   │
                   ▼
     [14.analytics.orders-by-user]
     (changelog: userId → total count)
```

**5-хв вікно — сума виручки per userId:**
```
orders.created
  .groupBy(userId)
  .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
  .aggregate({ 0.0 }, { _, v, agg -> agg + v.totalAmount })
  .toStream()
  .peek(log)
```

**Сервіси та порти (з docker-compose-14.yml):**
- `kafka` — 9092 (KRaft single-node)
- `schema-registry-b14` — 8090
- `kafka-ui` — 8080 → http://localhost:8080
- `order-service-b14` — 8081 → http://localhost:8081
- `analytics-service-b14` — 8084 → http://localhost:8084

---

## 4. Ключові концепції

### KStream vs KTable

```
KStream — append-only:
  offset=0: key=user-1, value={orderId=a}
  offset=1: key=user-2, value={orderId=b}
  offset=2: key=user-1, value={orderId=c}  ← нова подія, не оновлення

KTable — upsert (latest per key):
  key=user-1 → 2  (після двох замовлень)
  key=user-2 → 1

KTable == "знімок поточного стану", KStream == "журнал всіх подій"
```

### State Store (RocksDB)

```
Kafka Streams зберігає стан локально:
  Тип: RocksDB (embedded key-value)
  Ім'я: "orders-by-user-store"
  Відновлення: при рестарті читає changelog topic з початку

Доступ через Interactive Queries:
  val store = streams.store(
      StoreQueryParameters.fromNameAndType(
          "orders-by-user-store",
          QueryableStoreTypes.keyValueStore()
      )
  )
  store.get("user-1")  // → 5L
```

### Tumbling Window

```
TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)):

[0:00─1:00)  [1:00─2:00)  [2:00─3:00)  ...
   3 orders     5 orders     2 orders

Кожне вікно незалежне, без overlap.
"WithNoGrace" = не чекаємо на late-arriving events.

Hopping Window (для порівняння):
TimeWindows.of(Duration.ofMinutes(5)).advanceBy(Duration.ofMinutes(1))
  → вікна перекриваються, кожна хвилина з'являється нове вікно
```

### @EnableKafkaStreams + StreamsConfig

```kotlin
@Configuration
@EnableKafkaStreams
class KafkaStreamsConfig {
    @Bean(name = [KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME])
    fun kStreamsConfig(): KafkaStreamsConfiguration {
        return KafkaStreamsConfiguration(mapOf(
            StreamsConfig.APPLICATION_ID_CONFIG to "analytics-service",
            StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG to SpecificAvroSerde::class.java,
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryUrl
        ))
    }
}
```

`APPLICATION_ID_CONFIG` — ідентифікатор застосунку. Kafka Streams автоматично створює
internal topics: `analytics-service-orders-by-user-store-changelog`.

### Filter → нова гілка потоку

```kotlin
// Висока цінність замовлень → окремий топік
ordersStream
    .filter { _, v -> v.totalAmount >= 50.0 }
    .to("14.analytics.orders-high-value")

// Без state store — stateless операція, дуже ефективна
```

---

## 5. Як запустити

```bash
# Запустити
docker compose -f docker-compose-14.yml up --build

# Kafka UI: http://localhost:8080
# analytics-service REST: http://localhost:8084
```

**Перевірка готовності:**
```bash
# Переконатись що Kafka Streams перейшов у стан RUNNING
curl -s http://localhost:8084/api/analytics/orders-by-user | jq .
# {} (порожньо до першого замовлення)
```

---

## 6. Як тестувати

### Крок 1 — Надіслати пакет замовлень

```bash
# 20 замовлень для 3 користувачів
curl -s -X POST "http://localhost:8081/api/orders/batch?users=alice,bob,charlie&count=20"
```

### Крок 2 — State Store: підрахунок per userId

```bash
curl -s http://localhost:8084/api/analytics/orders-by-user | jq .
# {"alice": 7, "bob": 8, "charlie": 5}  (розподіл залежить від rnd)
```

### Крок 3 — Топ-3 users

```bash
curl -s "http://localhost:8084/api/analytics/top-users?limit=3" | jq .
# [{"userId":"bob","count":8}, {"userId":"alice","count":7}, ...]
```

### Крок 4 — Tumbling window у Kafka UI

```bash
# Відкрити http://localhost:8080 → Topics → 14.analytics.orders-by-user
# Повідомлення: key=alice, value=7 (оновлюється кожне нове замовлення)

# Додати одне замовлення з totalAmount >= $50
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"alice","itemCount":3,"category":"ELECTRONICS","totalAmount":150.0}' | jq .

# Топік 14.analytics.orders-high-value отримає цю подію
```

### Крок 5 — Дивитись logs analytics-service

```bash
docker logs analytics-service-b14 -f --tail 30
# [WINDOW-1MIN] category=ELECTRONICS count=5 window=[0:00-1:00)
# [WINDOW-5MIN] userId=alice revenue=450.0 window=[0:00-5:00)
# [HIGH-VALUE] orderId=... totalAmount=150.0
```

---

## 7. Поглиблений розгляд

### Topology внутрішньо

```
Source → ... → Sink
analytics-service topology має 4 гілки:
  1. Source(14.orders.created)
     → SelectKey(userId)
     → GroupBy
     → Count          → StateStore("orders-by-user-store")
     → ToStream
     → Sink(14.analytics.orders-by-user)

  2. Source(14.orders.created)
     → SelectKey(category)
     → GroupBy
     → WindowedBy(1 хв)
     → Count
     → ToStream
     → Peek(log)

  3. Source(14.orders.created)
     → SelectKey(userId)
     → GroupBy
     → WindowedBy(5 хв)
     → Aggregate(sum totalAmount)
     → ToStream
     → Peek(log)

  4. Source(14.orders.created)
     → Filter(totalAmount >= 50)
     → Sink(14.analytics.orders-high-value)
```

### Co-partitioning requirement

```
KStream-KTable join вимагає co-partitioning:
  - однакова кількість партицій
  - однаковий ключ (для routing до того самого Streams instance)

У нашому прикладі groupBy(userId) re-keys потік → новий internal topic
Kafka Streams автоматично створює: analytics-service-KSTREAM-AGGREGATE-STATE-STORE-...
```

### Exactly-once через state store

```
При збої Streams instance:
  1. Перезапуск → читає changelog topic від початку
  2. Відновлює RocksDB state store до останнього стану
  3. Починає обробку з committed offset

→ Результат: at-least-once з idempotent count (count не дублюється через KTable semantics)
```

---

## 8. Структура проекту

```
branch14_streams/
├── order-service/
│   ├── controller/OrderController.kt         ← POST /api/orders, POST /api/orders/batch
│   └── src/main/avro/OrderCreatedEvent.avsc  ← додано: category, totalAmount
└── analytics-service/
    ├── config/
    │   ├── KafkaStreamsConfig.kt              ← @EnableKafkaStreams, StreamsConfig
    │   └── KafkaTopicConfig.kt               ← створення output topics
    ├── streams/
    │   └── OrderAnalyticsTopology.kt         ← вся Streams топологія (4 гілки)
    └── controller/AnalyticsController.kt     ← GET /api/analytics/orders-by-user
                                                 GET /api/analytics/top-users
```

---

## 9. Що далі

- **Branch 15** — Kafka Connect: замість Spring Kafka consumer — Debezium CDC connector
  читає зміни з PostgreSQL WAL і публікує в Kafka без коду.
- **Branch 20** — Kafka Streams Advanced: KStream-KTable join, KStream-GlobalKTable join,
  KStream-KStream windowed join, EOS (exactly_once_v2).

---

## 10. Слайди для лекції

### Слайд 1 — KStream vs KTable

```
KStream (append-only):           KTable (upsert):
  [a, 1]                           user-1 → 2
  [b, 1]     groupBy.count()  →    user-2 → 1
  [a, 1]  ──────────────────►
           (changelog topic)

KTable backed by state store (RocksDB)
```

### Слайд 2 — Tumbling Window

```
  events:  ●   ●  ●●   ●     ●●●   ●  ●
time: ─────[──0:00─────1:00──]────[──1:00─────2:00──]────►
             window 1: 4        window 2: 4

TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1))
"WithNoGrace" = no waiting for late arrivals
```

### Слайд 3 — Interactive Queries

```
Kafka Streams зберігає стан локально (RocksDB):

HTTP Request → analytics-service → store.get("user-1") → 5L
                                         ↑
                              RocksDB state store
                              (відновлюється з Kafka changelog при рестарті)
```

---

## 11. Демонстраційний сценарій

```bash
#!/bin/bash
echo "=== Branch 14: Kafka Streams Demo ==="

docker compose -f docker-compose-14.yml up --build -d
echo "Очікуємо 2 хв..."
sleep 120

echo ""
echo "=== Крок 1: Генерація замовлень ==="
curl -s -X POST "http://localhost:8081/api/orders/batch?users=alice,bob,charlie&count=15"
sleep 5

echo ""
echo "=== Крок 2: State Store — підрахунок per user ==="
curl -s http://localhost:8084/api/analytics/orders-by-user | jq .

echo ""
echo "=== Крок 3: Топ-3 користувачів ==="
curl -s "http://localhost:8084/api/analytics/top-users?limit=3" | jq .

echo ""
echo "=== Крок 4: High-value замовлення ==="
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"vip","itemCount":5,"category":"ELECTRONICS","totalAmount":999.0}'
sleep 2
echo "Kafka UI → topic 14.analytics.orders-high-value"

echo ""
echo "=== Logs: Tumbling window aggregations ==="
docker logs analytics-service-b14 --tail 20
```

---

## 12. Питання для самоперевірки

- Яка різниця між KStream і KTable на рівні семантики?
- Що таке state store і де він фізично зберігається?
- Чому `groupBy` у Kafka Streams може спричинити re-partitioning?
- Що відбувається зі state store при рестарті analytics-service?
- Tumbling window vs Hopping window — у чому різниця?
- Навіщо `WithNoGrace` у `TimeWindows.ofSizeWithNoGrace()`?
- Що таке Interactive Queries і як вони працюють у multi-instance deployments?
- Чому analytics-service не потребує Schema Registry при читанні, якщо він вже читає Avro?