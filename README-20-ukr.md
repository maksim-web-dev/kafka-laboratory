# Branch 20 — Kafka Streams Advanced: Joins, GlobalKTable, EOS

## 1. Що вивчаємо у цій гілці

- **KStream-KTable Join** — збагачення потоку подій даними з таблиці (co-partitioned by key).
- **KStream-GlobalKTable Join** — збагачення без co-partitioning (key extractor lambda).
- **KStream-KStream Windowed Join** — зіставлення двох потоків у часовому вікні (5 хв).
- **GlobalKTable vs KTable** — повна реплікація на кожний Streams instance vs партиціонована.
- **`selectKey()`** — re-keying потоку для co-partitioning перед KTable join.
- **EOS (Exactly-Once Semantics)** — `processing.guarantee=exactly_once_v2`.

---

## 2. Зміни порівняно з попередньою гілкою (branch19)

- **Повернуто** single-broker (без SASL/ACL) — фокус на Kafka Streams складних операціях.
- **Додано** user-service — новий мікросервіс для управління профілями користувачів та продуктами.
- **analytics-service** розширено: тепер 3 типи join (KTable, GlobalKTable, KStream-KStream).
- **Нові топіки**: `20.users`, `20.products`, `20.payments.processed`,
  `20.enriched.orders`, `20.enriched.products`, `20.matched.orders`.
- **EOS** увімкнено у analytics-service: `processing.guarantee: exactly_once_v2`.

---

## 3. Архітектура

```
order-service  ──[20.orders.created]─────────────────────────────► analytics-service
               ──[20.payments.processed]─────────────────────────► (Kafka Streams)
user-service   ──[20.users (KTable)]────────────────────────────►       │
               ──[20.products (GlobalKTable)]───────────────────►       │
                                                                         │
                                             [20.enriched.orders]  ◄── KStream-KTable join
                                             [20.enriched.products]◄── KStream-GlobalKTable join
                                             [20.matched.orders]   ◄── KStream-KStream join
```

**Сервіси та порти (з docker-compose-20.yml):**
- `kafka` — 9092 (KRaft single-node)
- `kafka-ui` — 8080 → http://localhost:8080
- `order-service-b20` — 8081 → http://localhost:8081
- `user-service-b20` — 8086 → http://localhost:8086
- `analytics-service-b20` — 8084 → http://localhost:8084

---

## 4. Ключові концепції

### KStream-KTable Join (co-partitioning required)

```
Проблема:
  orders stream (key=orderId) JOIN users KTable (key=userId)
  → різні ключі! Потрібен re-key.

Рішення: selectKey() перед join
  ordersStream
    .selectKey { _, v -> v.userId }     ← re-key by userId
    .join(usersTable) { order, user ->   ← KTable join
        EnrichedOrder(order, user.name, user.tier)
    }
    .to("20.enriched.orders")

Co-partitioning вимога:
  - orders та users МАЮТЬ мати однакову кількість партицій
  - selectKey() → internal repartition topic (автоматично)
```

### KStream-GlobalKTable Join (no co-partitioning)

```
GlobalKTable:
  - Кожен Streams instance читає ВСІ партиції → повна копія на кожному вузлі
  - Co-partitioning НЕ потрібно
  - Key extractor lambda замість selectKey

ordersStream
  .join(productsGlobalTable,
      { _, order -> order.productId },   ← key extractor: береться productId з order
      { order, product ->
          EnrichedProduct(order, product.name, product.category)
      }
  )
  .to("20.enriched.products")

Різниця від KTable:
  KTable: кожен instance читає свої партиції (memory efficient)
  GlobalKTable: кожен instance читає все (memory intensive, але без co-partitioning)
```

### KStream-KStream Windowed Join

```
Проблема: orders та payments — два потоки; хочемо зіставити order з його payment

Рішення: JoinWindows визначає часовий діапазон

ordersStream
  .join(paymentsStream,
      { order, payment -> MatchedOrder(order, payment.status) },
      JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5))
  )
  .to("20.matched.orders")

order t=0:00 ↔ payment t=0:00..4:59 → MATCH ✓
order t=0:00 ↔ payment t=5:01       → NO MATCH ✗

Обидва потоки мають однаковий key (orderId) → co-partitioned
```

### EOS (Exactly-Once Semantics)

```yaml
spring:
  kafka:
    streams:
      properties:
        processing.guarantee: exactly_once_v2
```

```
exactly_once_v2:
  1. Transactional producer (transactional.id = {app-id}-{partition})
  2. Read-process-write як єдина транзакція
  3. При збої → транзакція відкочується → повідомлення перечитується
  4. Результат: кожне вхідне повідомлення обробляється рівно один раз

Vs exactly_once (deprecated): повільніше (distributed transactions)
Vs at_least_once (default): можливі дублікати при збої після write, перед commit
```

---

## 5. Як запустити

```bash
docker compose -f docker-compose-20.yml up --build

# Kafka UI: http://localhost:8080
```

---

## 6. Як тестувати

### Крок 1 — Заповнити KTable (users)

```bash
# Додати профілі користувачів
for tier in GOLD SILVER BRONZE; do
  for i in 1 2 3; do
    curl -s -X POST http://localhost:8086/api/users \
      -H "Content-Type: application/json" \
      -d "{\"userId\":\"user-$i\",\"name\":\"User $i\",\"email\":\"user$i@example.com\",\"tier\":\"$tier\"}" \
      | jq -r '.userId'
  done
done

# Переглянути
curl -s http://localhost:8086/api/users | jq .
```

### Крок 2 — KStream-KTable Join (orders ← users)

```bash
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-1","productId":"prod-laptop","quantity":1,"totalAmount":1500.0}' | jq .

# Логи analytics-service:
# [JOIN-KTABLE] orderId=... userId=user-1 userName=User 1 userTier=GOLD
# Kafka UI → topic 20.enriched.orders
```

### Крок 3 — KStream-GlobalKTable Join (orders ← products)

```bash
# Додати продукт до GlobalKTable
curl -s -X POST http://localhost:8086/api/products \
  -H "Content-Type: application/json" \
  -d '{"productId":"prod-laptop","name":"Laptop Pro","category":"ELECTRONICS"}' | jq .

# Замовлення збагатиться назвою продукту
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-2","productId":"prod-laptop","quantity":1,"totalAmount":1500.0}' | jq .

# Логи: [JOIN-GLOBAL] orderId=... productId=prod-laptop productName=Laptop Pro
# Kafka UI → topic 20.enriched.products
```

### Крок 4 — KStream-KStream Windowed Join

```bash
# order-service одразу публікує і payment
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-2","productId":"prod-phone","quantity":2,"totalAmount":800.0}' | jq .

# Логи: [JOIN-STREAM] orderId=... amount=800.0 paymentStatus=APPROVED matchedAt=...
# Kafka UI → topic 20.matched.orders
```

### Крок 5 — Batch test

```bash
curl -s -X POST "http://localhost:8081/api/orders/batch?users=user-1,user-2,user-3&count=10" | jq .
```

---

## 7. Поглиблений розгляд

### co-partitioning та selectKey()

```
Правило co-partitioning для KTable join:
  - joined streams МАЮТЬ мати однакову кількість партицій
  - joined streams МАЮТЬ мати однаковий partition key

ordersStream ключ=orderId, usersTable ключ=userId
→ потрібен selectKey() → repartition (internal topic)
→ Kafka Streams автоматично відправляє повідомлення до правильного instance

Без selectKey() → IllegalArgumentException при топологія validation
```

### GlobalKTable вартість

```
KTable (regular):
  Кожен instance читає M/N партицій (N = кількість instances)
  Memory: state size / N
  Join: тільки з co-partitioned streams

GlobalKTable:
  Кожен instance читає ВСІ партиції
  Memory: full state size на кожному instance
  Join: з будь-яким stream (key extractor)

GlobalKTable підходить для:
  - невеликих таблиць (конфігурація, продукти, користувачі)
  - коли co-partitioning незручний або неможливий
```

### KTable changelog topic

```
KTable backed by internal Kafka topic:
  analytics-service-KSTREAM-KEY-SELECT-... ← repartition topic
  analytics-service-...-STATE-STORE-changelog ← state backup

При рестарті:
  Streams читає changelog topic → відновлює RocksDB state store
  → приєднується до обробки без втрати стану
```

---

## 8. Структура проекту

```
branch20_streams_advanced/
├── order-service/
│   ├── controller/OrderController.kt      ← POST /api/orders, POST /api/orders/batch
│   └── service/OrderService.kt            ← публікує 20.orders.created + 20.payments.processed
├── user-service/
│   ├── controller/UserController.kt       ← POST /api/users, POST /api/products
│   └── service/UserService.kt             ← публікує 20.users, 20.products
└── analytics-service/
    ├── config/KafkaStreamsConfig.kt        ← @EnableKafkaStreams, EOS config
    └── streams/
        ├── OrderEnrichmentTopology.kt      ← KStream-KTable join → 20.enriched.orders
        ├── ProductEnrichmentTopology.kt    ← KStream-GlobalKTable join → 20.enriched.products
        └── OrderPaymentMatchTopology.kt    ← KStream-KStream windowed → 20.matched.orders
```

---

## 9. Що далі

- **Branch 21** — Log Compaction: `cleanup.policy=compact`, tombstone records,
  state reconstruction через compacted topic.

---

## 10. Слайди для лекції

### Слайд 1 — KTable vs GlobalKTable

```
KTable (partitioned):              GlobalKTable:
  instance-1: userId=1..500          instance-1: userId=1..1000 (ALL)
  instance-2: userId=501..1000       instance-2: userId=1..1000 (ALL)

  Memory: N/2 each                   Memory: N each (2x)
  Join: co-partitioned only          Join: any stream (key extractor)
```

### Слайд 2 — Windowed Join

```
  orders:   ──●────────────────────────────►
                orderId=A, t=0:00
  payments: ──────────●───────────────────►
                      orderId=A, t=0:03

  JoinWindows(5 min): |0:00 - 0:03| = 3 min < 5 min → MATCH ✓

  Result → 20.matched.orders: {orderId=A, paymentStatus=APPROVED}
```

### Слайд 3 — EOS exactly_once_v2

```
at_least_once (default):
  read(offset=5) → process → write(result) → commit(offset=6)
  Crash before commit → re-read offset=5 → duplicate result!

exactly_once_v2:
  Transaction: begin
    read(offset=5) → process → write(result) → commit(offset=6)
  Transaction: commit atomically
  Crash → rollback → re-read offset=5 → idempotent → no duplicate
```

---

## 11. Демонстраційний сценарій

```bash
#!/bin/bash
echo "=== Branch 20: Kafka Streams Advanced Demo ==="

docker compose -f docker-compose-20.yml up --build -d
sleep 90

echo ""
echo "=== Заповнення KTable (users) ==="
for i in 1 2 3; do
  curl -s -X POST http://localhost:8086/api/users \
    -H "Content-Type: application/json" \
    -d "{\"userId\":\"user-$i\",\"name\":\"User $i\",\"tier\":\"GOLD\"}"
done

echo ""
echo "=== Заповнення GlobalKTable (products) ==="
curl -s -X POST http://localhost:8086/api/products \
  -H "Content-Type: application/json" \
  -d '{"productId":"prod-laptop","name":"Laptop Pro","category":"ELECTRONICS"}'

echo ""
echo "=== KStream-KTable Join ==="
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-1","productId":"prod-laptop","quantity":1,"totalAmount":1500.0}' | jq .
sleep 3
echo "Kafka UI → topic 20.enriched.orders"

echo ""
echo "=== KStream-GlobalKTable Join ==="
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-2","productId":"prod-laptop","quantity":2,"totalAmount":3000.0}' | jq .
sleep 3
echo "Kafka UI → topic 20.enriched.products"

echo ""
echo "=== KStream-KStream Windowed Join ==="
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-3","productId":"prod-phone","quantity":1,"totalAmount":800.0}' | jq .
sleep 3
echo "Kafka UI → topic 20.matched.orders"
```

---

## 12. Питання для самоперевірки

- Що таке co-partitioning і чому воно потрібне для KStream-KTable join?
- Яка різниця між KTable та GlobalKTable по пам'яті та join можливостях?
- Навіщо `selectKey()` перед join і що відбувається після нього?
- Що таке JoinWindows і чому `WithNoGrace`?
- Яка різниця між `at_least_once` та `exactly_once_v2` при збої?
- Що таке KTable changelog topic і як відновлюється стан при рестарті?
- Коли використовувати GlobalKTable замість KTable?
- Що відбувається якщо user-service відправить оновлення профілю? Чи оновиться KTable?