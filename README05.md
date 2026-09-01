# Branch 05 — Multiple Consumer Groups

## Що змінилося порівняно з branch04

| Аспект | branch04 | branch05 |
|--------|----------|----------|
| Сервіси | order + 3×notification | order + notification + **analytics** |
| Consumer groups | `notification-service-group` | `notification-service-group` + **`analytics-service-group`** |
| notification-service | 3 інстанси | **1 інстанс** |
| Новий сервіс | — | **`analytics-service`** |
| analytics endpoints | — | `GET /api/analytics/stats` |
| | — | `GET /api/analytics/stats/user/{userId}` |
| Порти | 9098/8088/8095–8098 | **9100/8090/8101/8102/8103** |

---

## Архітектура

```
                    topic: orders.created (3 partitions)
                           │
          ┌────────────────┴────────────────┐
          │                                 │
          ▼                                 ▼
notification-service-group        analytics-service-group
  notification-service-b05          analytics-service-b05
  :8102                              :8103
  → надсилає email                   → рахує статистику
  offset: незалежний                 offset: незалежний
```

### Ключовий принцип

> Кожна consumer group має **власний offset** у кожній партиції.
> Читання однією групою не впливає на offset іншої.

```
__consumer_offsets (внутрішній топік Kafka):

notification-service-group | orders.created | partition 0 → offset 15
notification-service-group | orders.created | partition 1 → offset 12
notification-service-group | orders.created | partition 2 → offset 18

analytics-service-group    | orders.created | partition 0 → offset 8   ← відстає!
analytics-service-group    | orders.created | partition 1 → offset 12
analytics-service-group    | orders.created | partition 2 → offset 18
```

---

## Новий сервіс: analytics-service

### Що робить

Слухає `orders.created` як незалежний consumer (`analytics-service-group`) і накопичує:
- Загальна кількість замовлень
- Загальна виручка
- Кількість замовлень per user
- Виручка per user

### Endpoints

| Метод | URL | Опис |
|-------|-----|------|
| GET | `/api/analytics/stats` | Загальна статистика |
| GET | `/api/analytics/stats/user/{userId}` | Статистика конкретного user |

---

## Як запустити

```bash
# Зупинити branch04
docker stop kafka-b04 kafka-ui-b04 order-service-b04 \
  notification-service-1-b04 notification-service-2-b04 notification-service-3-b04 2>/dev/null || true

# Запустити branch05
docker compose -f docker-compose-05.yml down -v 2>/dev/null || true
docker compose -f docker-compose-05.yml up --build
```

Перевірити 5 контейнерів:

```bash
docker compose -f docker-compose-05.yml ps
# kafka, kafka-ui, order-service-b05
# notification-service-b05, analytics-service-b05
```

---

## Як протестувати

### 1. Надіслати кілька замовлень

```bash
for user in user-01 user-02 user-01 user-03 user-02 user-01; do
  curl -s -X POST http://localhost:8101/api/orders \
    -H "Content-Type: application/json" \
    -d "{\"userId\":\"$user\",\"product\":\"Book\",\"quantity\":1,\"totalAmount\":25.00}" \
    | jq -r '.userId + " → " + .orderId'
done
```

### 2. Перевірити що обидві групи отримали всі події

```bash
# notification-service — email-сповіщення
curl -s http://localhost:8102/api/notifications/count | jq
# {"instanceId":"notification-service-1","orders_created":6,"orders_cancelled":0,"total":6}

# analytics-service — статистика
curl -s http://localhost:8103/api/analytics/stats | jq
```

Очікувана відповідь analytics:

```json
{
  "consumerGroup": "analytics-service-group",
  "totalOrders": 6,
  "totalRevenue": "150.00",
  "uniqueUsers": 3,
  "ordersByUser": {
    "user-01": 3,
    "user-02": 2,
    "user-03": 1
  },
  "revenueByUser": {
    "user-01": "75.00",
    "user-02": "50.00",
    "user-03": "25.00"
  }
}
```

### 3. Головний демо-сценарій: зупинити analytics, надіслати повідомлення, запустити

```bash
# Крок 1: Зупинити analytics-service
docker stop analytics-service-b05
echo "Analytics зупинено"

# Крок 2: Надіслати 5 нових замовлень
for i in 1 2 3 4 5; do
  curl -s -X POST http://localhost:8101/api/orders \
    -H "Content-Type: application/json" \
    -d "{\"userId\":\"user-missed\",\"product\":\"Missed Order #$i\",\"quantity\":1,\"totalAmount\":10.00}" \
    | jq -r '"Sent: " + .orderId'
done

# Крок 3: Notification отримала всі 5 (поки analytics спав)
curl -s http://localhost:8102/api/notifications/count | jq
# total: 11

# Крок 4: Запустити analytics знову
docker start analytics-service-b05
sleep 5  # чекаємо підключення

# Крок 5: Analytics дочитав пропущені 5 повідомлень
curl -s http://localhost:8103/api/analytics/stats | jq '.totalOrders, .ordersByUser'
# totalOrders: 11  ← дочитав всі
# ordersByUser.user-missed: 5  ← обробив пропущені
```

### 4. Порівняти lag через CLI

```bash
# Поки analytics зупинений:
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group analytics-service-group

# Очікуваний LAG > 0 — analytics відстає
# GROUP                    TOPIC           PARTITION  OFFSET  LOG-END  LAG
# analytics-service-group  orders.created  0          2       4        2   ← відстає
# analytics-service-group  orders.created  1          3       5        2
# analytics-service-group  orders.created  2          1       3        2
```

```bash
# notification-service не відстає
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group notification-service-group
# LAG = 0 для всіх партицій
```

### 5. Перевірити статистику per user

```bash
curl -s http://localhost:8103/api/analytics/stats/user/user-01 | jq
# {"userId":"user-01","orders":3,"revenue":"75.00"}
```

### 6. Kafka UI — бачимо два Consumer Groups

Відкрити: [http://localhost:8090](http://localhost:8090)

- `Consumer Groups` → бачимо **два** записи:
  - `notification-service-group` (lag = 0, 1 member)
  - `analytics-service-group` (lag = 0 або > 0 якщо зупинений)
- Порівняти offsets кожної групи — вони незалежні

---

## Як це працює всередині

### Чому два сервіси отримують одні й ті самі повідомлення?

Kafka **не видаляє** повідомлення після прочитання (на відміну від черг RabbitMQ).
Повідомлення зберігаються до закінчення `retention.ms` (7 днів для `orders.created`).

```
Producer → orders.created[partition 1]:
  offset 0: OrderCreated{userId="user-42"}
  offset 1: OrderCreated{userId="user-99"}
  offset 2: OrderCreated{userId="user-42"}

notification-service-group читає partition 1:
  offset 0 ✓ → email sent
  offset 1 ✓ → email sent
  committed offset = 2

analytics-service-group читає той самий partition 1:
  offset 0 ✓ → counted
  offset 1 ✓ → counted
  committed offset = 2
  (незалежно від notification)
```

### Replay: читання з початку для нової групи

Якщо запустити нову consumer group (або скинути offset через CLI):

```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group analytics-service-group \
  --topic orders.created \
  --reset-offsets --to-earliest --execute
```

Analytics-service при наступному старті прочитає **всі** повідомлення з початку топіку.
Це дозволяє "перегравати" події для нових сервісів або після помилки.

### analytics-service application.yml

```yaml
spring:
  kafka:
    consumer:
      group-id: analytics-service-group   # ← інша group, ніж у notification
      auto-offset-reset: earliest          # ← читати з початку при першому старті
```

`auto-offset-reset: earliest` важливо для analytics — інакше нові замовлення, що надійшли
до першого підключення analytics, були б пропущені.

---

## Структура проєкту (зміни відносно branch04)

```
kafka-laboratory/
├── docker-compose-05.yml                 ← 1 notification (не 3), + analytics-service
├── branch05_multiple_consumer_groups/
│   ├── analytics-service/                ← NEW сервіс
│   │   ├── Dockerfile
│   │   ├── build.gradle.kts
│   │   ├── settings.gradle.kts
│   │   └── src/main/kotlin/com/kafkalab/analytics/
│   │       ├── AnalyticsServiceApplication.kt
│   │       ├── controller/AnalyticsController.kt   ← /stats, /stats/user/{id}
│   │       ├── listener/OrderAnalyticsListener.kt  ← analytics-service-group
│   │       └── model/OrderCreatedEvent.kt
│   │   └── src/main/resources/application.yml     ← group-id: analytics-service-group
│   ├── notification-service/             ← 1 інстанс (спрощено від branch04)
│   └── order-service/                    ← топіки 05.orders.*
└── README05.md
```

---

## Ключові концепції цієї гілки

| Концепція | Що демонструє приклад |
|-----------|----------------------|
| **Multiple Consumer Groups** | 2 незалежних групи читають той самий топік |
| **Independent Offsets** | notification offset ≠ analytics offset |
| **No data loss** | Analytics дочитує пропущені події після рестарту |
| **Replay** | `--reset-offsets --to-earliest` → перечитати всі події |
| **Fan-out** | Один producer → N споживачів без додаткових копій даних |
| **auto-offset-reset: earliest** | Нова group читає з початку топіку (не пропускає старі) |

---

## Що далі — branch06

У наступній гілці вивчаємо **Offsets & Commits**:
- Auto commit (`enable.auto.commit=true`) — ризики та налаштування
- Manual commit — `commitSync()` vs `commitAsync()`
- Offset reset strategies: `earliest`, `latest`, `none`
- At-least-once vs at-most-once delivery
- Симуляція збою під час обробки