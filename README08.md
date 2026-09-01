# Branch 08 — Consumer Configuration

## Що змінилося порівняно з branch07

| Аспект | branch07 | branch08 |
|--------|----------|----------|
| Assignment strategy | `RangeAssignor` | **`CooperativeStickyAssignor`** |
| Rebalance тип | EAGER (stop-the-world) | **COOPERATIVE (incremental)** |
| Static membership | — | **`group.instance.id` per listener** |
| `onPartitionsRevoked` log | REVOKED всі | **REVOKED тільки переїжджаючі** |
| Graceful shutdown | — | **`server.shutdown=graceful`** |
| Poll tuning | defaults | **`max.poll.records`, `max.poll.interval.ms`** |
| Session tuning | defaults | **`session.timeout.ms`, `heartbeat.interval.ms`** |
| Notification instances | 1 | **2** |
| Порти | 9104/8108/8109/8110 | **9106/8112/8113/8114/8115** |

---

## Ключові концепції

### EAGER vs COOPERATIVE Rebalance

#### EAGER (branch04 — RangeAssignor)

```
Rebalance triggered (новий інстанс приєднується):
  1. ВСІ consumers: REVOKED усі партиції → stop consuming
  2. Group coordinator: призначає нові партиції
  3. ВСІ consumers: ASSIGNED нові партиції → resume consuming
  
Stop-the-world: весь consumer group не читає під час rebalance
```

#### COOPERATIVE (branch08 — CooperativeStickyAssignor)

```
Rebalance triggered (новий інстанс приєднується):
  Round 1:
    - consumers повідомляють що мають
    - coordinator визначає що треба перерозподілити
    - тільки "переїжджаючі" партиції REVOKED
  Round 2:
    - нові assignments ASSIGNED
    - інші партиції продовжують читатись БЕЗ перерви

Тільки частина партицій призупиняється → мінімальний downtime
```

### Порівняння callback-логів

EAGER (branch04):
```
[REBALANCE] REVOKED:  [orders.created[0], orders.created[1], orders.created[2]]  ← все
[REBALANCE] ASSIGNED: [orders.created[0], orders.created[1]]
```

COOPERATIVE (branch08):
```
[COOPERATIVE] REVOKED:  [orders.created[2]]   ← тільки партиція що "переїжджає"
[COOPERATIVE] ASSIGNED: [orders.created[2]]   ← новий інстанс отримує її
```

### Static Group Membership

```
Без group.instance.id (dynamic membership):
  instance-1 перезапускається → broker: "member залишив групу" → rebalance одразу
  instance-1 підключається → rebalance знову

З group.instance.id (static membership):
  instance-1 перезапускається → broker: "member offline"
  Broker чекає session.timeout.ms (45s) перш ніж оголосити dead
  instance-1 повертається до session.timeout → NO rebalance! ← ключова перевага
```

Кожен `@KafkaListener` отримує унікальний `group.instance.id`:
```
notification-1 listener orders.created  → group.instance.id = "notification-1-orders-created"
notification-1 listener orders.cancelled → group.instance.id = "notification-1-orders-cancelled"
notification-2 listener orders.created  → group.instance.id = "notification-2-orders-created"
```

### max.poll.records та max.poll.interval.ms

```
max.poll.records=10:
  Consumer читає щонайбільше 10 записів за один poll()
  Менше = менший час обробки batch → менший ризик перевищення max.poll.interval

max.poll.interval.ms=300000 (5 хв):
  Якщо між двома poll() > 5 хв → broker виключає consumer з групи → rebalance
  
Типова проблема: обробка одного batch займає > max.poll.interval → consumer виключається
Рішення: зменшити max.poll.records або збільшити max.poll.interval.ms
```

### session.timeout.ms та heartbeat.interval.ms

```
heartbeat.interval.ms=15000 (15с):
  Consumer надсилає heartbeat кожні 15с
  Правило: heartbeat.interval < session.timeout / 3

session.timeout.ms=45000 (45с):
  Broker визнає consumer dead якщо не отримав heartbeat протягом 45с
  Менше = швидше виявлення збою (але ризик false positive при GC pause)
  Більше = повільніше виявлення, але стабільніше
```

---

## Як запустити

```bash
# Зупинити branch07
docker stop kafka kafka-ui order-service-b07 notification-service-b07 2>/dev/null || true

# Запустити branch08
docker compose -f docker-compose-08.yml down -v 2>/dev/null || true
docker compose -f docker-compose-08.yml up --build
```

Перевірити 5 контейнерів:

```bash
docker compose -f docker-compose-08.yml ps
# kafka, kafka-ui, order-service-b08
# notification-service-1-b08 (port 8114)
# notification-service-2-b08 (port 8115)
```

---

## Як протестувати

### 1. Перевірити початковий розподіл партицій (2 інстанси, 3 партиції)

```bash
# RangeAssignor (branch04): instance-1 → [0,1], instance-2 → [2]
# CooperativeStickyAssignor: розподіл аналогічний, але rebalance м'якший

curl -s http://localhost:8114/api/notifications/partitions | jq
# {"instanceId":"notification-1","assignedPartitions":["orders.created[0]","orders.created[1]"],"count":2}

curl -s http://localhost:8115/api/notifications/partitions | jq
# {"instanceId":"notification-2","assignedPartitions":["orders.created[2]"],"count":1}
```

### 2. Переглянути конфігурацію consumer

```bash
curl -s http://localhost:8114/api/notifications/config | jq
```

Очікувана відповідь:
```json
{
  "instanceId": "notification-1",
  "groupId": "notification-service-group",
  "staticMemberId_created": "notification-1-orders-created",
  "staticMemberId_cancelled": "notification-1-orders-cancelled",
  "assignmentStrategy": "CooperativeStickyAssignor",
  "maxPollRecords": 10,
  "maxPollIntervalMs": 300000,
  "sessionTimeoutMs": 45000,
  "heartbeatIntervalMs": 15000,
  "ackMode": "MANUAL_IMMEDIATE",
  "enableAutoCommit": false,
  "gracefulShutdown": true
}
```

### 3. Головний демо-сценарій: COOPERATIVE rebalance

```bash
# Крок 1: Надіслати кілька замовлень
for i in 1 2 3 4 5 6; do
  curl -s -X POST http://localhost:8113/api/orders \
    -H "Content-Type: application/json" \
    -d "{\"userId\":\"user-0$i\",\"product\":\"Book\",\"quantity\":1,\"totalAmount\":25.00}" | jq -r '.orderId'
done

# Крок 2: Зупинити notification-service-2 (graceful shutdown)
docker stop notification-service-2-b08

# У логах notification-service-2 побачимо:
# [GRACEFUL SHUTDOWN] Instance [notification-2] stopping — finishing current batch, committing offsets...
# [COOPERATIVE] Instance [notification-2] REVOKED: [orders.created[2]]

# У логах notification-service-1 побачимо ТІЛЬКИ нові партиції:
# [COOPERATIVE] Instance [notification-1] ASSIGNED: [orders.created[2]]
# (НЕ REVOKED існуючих партицій [0],[1] — це і є COOPERATIVE)

# Крок 3: Перевірити що notification-1 тепер читає всі 3 партиції
curl -s http://localhost:8114/api/notifications/partitions | jq

# Крок 4: Запустити notification-service-2 знову
docker start notification-service-2-b08
sleep 5

# З static membership: якщо instance-2 повернувся до session.timeout — rebalance мінімальний
# Без static: rebalance відбувся б двічі (disconnect + reconnect)
```

### 4. Перевірити static membership через CLI

```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group notification-service-group

# GROUP                    TOPIC           PARTITION  CONSUMER-ID                                    HOST
# notification-service-group  orders.created  0    notification-1-orders-created-...  /172.22.0.x
# notification-service-group  orders.created  1    notification-1-orders-created-...  /172.22.0.x
# notification-service-group  orders.created  2    notification-2-orders-created-...  /172.22.0.x
```

Поле `CONSUMER-ID` починається з `group.instance.id` → підтверджує static membership.

### 5. Kafka UI — Consumer Groups

Відкрити: [http://localhost:8112](http://localhost:8112)

- `Consumer Groups` → `notification-service-group`
- Members: бачимо `notification-1-orders-created`, `notification-2-orders-created` як member IDs
- Зупинити instance-2 → спостерігати затримку до rebalance (session.timeout.ms = 45s)

---

## Параметри та їх рекомендовані значення

| Параметр | Branch08 | Production рекомендація |
|----------|----------|------------------------|
| `partition.assignment.strategy` | CooperativeStickyAssignor | CooperativeStickyAssignor |
| `group.instance.id` | per-listener | per-instance + per-topic |
| `max.poll.records` | 10 | 10-500 (залежно від обробки) |
| `max.poll.interval.ms` | 300000 | 5-30 хв |
| `session.timeout.ms` | 45000 | 30-60s |
| `heartbeat.interval.ms` | 15000 | session.timeout / 3 |

---

## Структура змін відносно branch07

```
kafka-laboratory/
├── docker-compose-08.yml                              ← 2 notification instances, нові порти b08 (9106/8112/8113/8114/8115)
├── branch08_consumer_configuration/
│   ├── notification-service/
│   │   └── src/main/resources/application.yml        ← COOPERATIVE, static membership, poll tuning, graceful shutdown
│   │   └── src/main/kotlin/.../listener/
│   │       └── OrderEventListener.kt                 ← ConsumerSeekAware, group.instance.id, @PreDestroy
│   │   └── src/main/kotlin/.../controller/
│   │       └── NotificationController.kt             ← /count, /partitions, /config
│   └── order-service/                                ← топіки 08.orders.*
└── README08.md
```

---

## Ключові концепції цієї гілки

| Концепція | Що демонструє |
|-----------|---------------|
| **COOPERATIVE rebalance** | Тільки переназначені партиції призупиняються |
| **CooperativeStickyAssignor** | Мінімальні переміщення партицій при rebalance |
| **Static membership** | `group.instance.id` → менше rebalance при рестарті |
| **onPartitionsRevoked (COOPERATIVE)** | Викликається лише для партицій що переїжджають |
| **max.poll.records** | Обмеження розміру batch → захист від max.poll.interval перевищення |
| **session.timeout.ms** | Як швидко broker виявляє мертвого consumer |
| **Graceful shutdown** | SIGTERM → finish batch → commit → leave group |

---

## Що далі — branch09

У наступній гілці вивчаємо **Error Handling & Dead Letter Topic**:
- `@RetryableTopic` — автоматичні retry topics
- `DefaultErrorHandler` з backoff стратегією
- Dead Letter Topic (DLT): `orders.created.DLT`
- Моніторинг та повторна обробка DLQ