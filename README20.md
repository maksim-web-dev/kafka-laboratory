# Branch 20 — Kafka Streams Advanced: Joins, GlobalKTable, EOS

## Що вивчаємо

| Концепція | Деталі |
|-----------|--------|
| **KStream-KTable join** | Збагачення потоку замовлень даними користувача (co-partitioned by userId) |
| **KStream-GlobalKTable join** | Збагачення даними продукту без co-partitioning |
| **KStream-KStream windowed join** | Зіставлення замовлень з платежами у 5-хвилинному вікні |
| **GlobalKTable vs KTable** | Повна реплікація на кожний вузол vs партиціонована таблиця |
| **EOS (Exactly-Once Semantics)** | `processing.guarantee=exactly_once_v2` у Kafka Streams |

## Архітектура

```
order-service  ──[20.orders.created]──────────────────────►┐
               ──[20.payments.processed]──────────────────►│
user-service   ──[20.users (KTable)]──────────────────────►│ analytics-service
                                                            │  (Kafka Streams)
               ──[20.products (GlobalKTable)]──────────────►│
                                                            │
                                          [20.enriched.orders]◄─ KStream-KTable join
                                          [20.enriched.products]◄─ KStream-GlobalKTable join
                                          [20.matched.orders]◄─ KStream-KStream join
```

## Порти

| Сервіс | Порт |
|--------|------|
| Kafka (external) | 9125 |
| Kafka UI | 8197 |
| order-service | 8198 |
| user-service | 8199 |
| analytics-service | 8200 |

## Запуск

```bash
docker compose -f docker-compose-20.yml up --build
```

---

## Ключові концепції

### KTable vs GlobalKTable

```
KTable (regular):
  - Партиціонована: кожен Streams instance читає лише свої партиції
  - Co-partitioning: joined streams МАЮТЬ мати однакову кількість партицій та однаковий ключ
  - Менше пам'яті (частина стану на кожному instance)

  orders (key=orderId) JOIN users KTable (key=userId) → потрібен selectKey!
  ordersStream.selectKey { _, v -> v.userId }  ← re-key by userId ПЕРЕД join

GlobalKTable:
  - Кожен Streams instance читає ВСІ партиції → повна копія на кожному вузлі
  - Co-partitioning НЕ потрібно — довільний ключ для join (key extractor lambda)
  - Більше пам'яті, але простота використання

  orders (key=orderId) JOIN products GlobalKTable (key=productId):
    .join(productsGlobalTable, { _, order -> order.productId }) ← key extractor
```

### KStream-KStream Windowed Join

```
Проблема: два потоки (orders + payments) містять події що мають зіставлятись
Рішення: JoinWindows визначає часовий діапазон для зіставлення

orders JOIN payments з вікном 5 хвилин:
  order t=0:00 ↔ payment t=0:00 → 4:59 → MATCH ✓
  order t=0:00 ↔ payment t=5:01           → NO MATCH ✗

JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5))
→ зіставляє events що відрізняються не більше ніж на 5 хвилин
```

### EOS (Exactly-Once Semantics)

```yaml
spring:
  kafka:
    streams:
      properties:
        processing.guarantee: exactly_once_v2  # recommended for Kafka 2.5+
```

```
exactly_once_v2 використовує:
  1. Transactional producer (transactional.id = {app-id}-{partition})
  2. Read-process-write як єдина транзакція
  3. Якщо processing fails → транзакція відкочується → повідомлення перечитується
  4. Результат: кожне вхідне повідомлення обробляється рівно один раз

Vs exactly_once (deprecated): використовував distributed transactions (повільніше)
Vs at_least_once (default): можливі дублікати при збої після запису, перед commit
```

---

## Як протестувати

### 1. Заповнити довідники

```bash
# Додати профілі користувачів (для KTable join)
for tier in GOLD SILVER BRONZE; do
  for i in 1 2 3; do
    curl -s -X POST http://localhost:8199/api/users \
      -H "Content-Type: application/json" \
      -d "{\"userId\":\"user-$i\",\"name\":\"User $i\",\"email\":\"user$i@example.com\",\"tier\":\"$tier\"}" | jq -r '.userId'
  done
done

# Переглянути профілі
curl -s http://localhost:8199/api/users | jq .
```

### 2. KStream-KTable Join (orders ← users)

```bash
# Створити замовлення — analytics-service збагатить даними user
curl -s -X POST http://localhost:8198/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-1","productId":"prod-laptop","quantity":1,"totalAmount":1500.0}' | jq

# Логи analytics-service:
# [JOIN-KTABLE] orderId=... userId=user-1 userName=User 1 userTier=GOLD
```

### 3. KStream-GlobalKTable Join (orders ← products)

```bash
# Додати продукт до GlobalKTable
curl -s -X POST http://localhost:8199/api/products \
  -H "Content-Type: application/json" \
  -d '{"productId":"prod-laptop","name":"Laptop Pro","category":"ELECTRONICS"}' | jq

# Тепер замовлення збагатиться назвою продукту
# Логи: [JOIN-GLOBAL] orderId=... productId=prod-laptop productName=Laptop Pro
```

### 4. KStream-KStream Windowed Join (orders ↔ payments)

```bash
# Надіслати замовлення — order-service одразу публікує і payment
curl -s -X POST http://localhost:8198/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-2","productId":"prod-phone","quantity":2,"totalAmount":800.0}' | jq

# Логи analytics-service:
# [JOIN-STREAM] orderId=... amount=800.0 paymentStatus=APPROVED matchedAt=...
```

### 5. Batch test

```bash
curl -s -X POST "http://localhost:8198/api/orders/batch?users=user-1,user-2,user-3&count=10" | jq
```

### 6. Перевірити enriched topics у Kafka UI

Відкрити: http://localhost:8197

- `20.enriched.orders` — KStream-KTable join результат
- `20.enriched.products` — KStream-GlobalKTable join результат
- `20.matched.orders` — KStream-KStream windowed join результат

---

## Ключові концепції

| Концепція | Що демонструє |
|-----------|---------------|
| **KStream-KTable join** | Non-windowed join; KTable — lookup side; потрібен selectKey для co-partitioning |
| **KStream-GlobalKTable join** | Без co-partitioning; key extractor lambda; кожен instance має повну копію |
| **KStream-KStream windowed join** | JoinWindows.ofTimeDifferenceWithNoGrace(Duration); обидва потоки є stream |
| **selectKey()** | Re-key stream для co-partitioning перед KTable join (змінює partition routing!) |
| **GlobalKTable** | Reads ALL partitions → full state on every instance; більше пам'яті |
| **EOS exactly_once_v2** | Transactional read-process-write; no duplicates при збої |
| **KTable changelog** | KTable backed by internal Kafka topic; state відновлюється після рестарту |