# Branch 03 — Message Keys

## Що змінилося порівняно з branch02

| Аспект | branch02 | branch03 |
|--------|----------|----------|
| Ключ `orders.created` | `orderId` (UUID, random) | **`userId`** (детермінований) |
| Ключ `orders.cancelled` | `orderId` | **`userId`** |
| Нові endpoint-и | — | `POST /api/orders/demo/keyed` |
|  | — | `POST /api/orders/demo/round-robin` |
| Нові моделі | — | `BatchOrderRequest`, `OrderSendResult` |
| Відповідь демо | — | `[{orderId, userId, key, partition, offset}, ...]` |
| Лог notification | `Partition: X  Offset: Y` | **`Key: user-42  →  Partition: X  Offset: Y`** |
| Docker container names | `b02-order-service`, `b02-notification-service` | `b03-order-service`, `b03-notification-service` |
| Зовнішні порти сервісів | 8091, 8092 | **8093, 8094** |
| Kafka та Kafka UI | `kafka` (9092), `kafka-ui` (8080) | ті самі (спільні) |

---

## Архітектура (акцент на ключах)

```
POST /api/orders              key = userId  ─┐
POST /api/orders/demo/keyed   key = userId  ─┤─── 03.orders.created (3 partitions)
POST /api/orders/demo/round-robin  key=null ─┘

hash(userId) % 3 → partition 0, 1, або 2

user-42  → hash("user-42") % 3 = 1  → partition 1  (завжди!)
user-99  → hash("user-99") % 3 = 0  → partition 0  (завжди!)
null key →  sticky partitioner      → round-robin   (непередбачувано)
```

---

## Ключова концепція: як key визначає партицію

```
Kafka DefaultPartitioner (Murmur2 hash):

partition = Math.abs(murmur2(key.getBytes())) % numPartitions

key="user-42"  → murmur2 → 0x7f3a1b2c → abs % 3 = 1
key="user-42"  → той самий хеш → та сама партиція → ЗАВЖДИ partition 1

key=null       → StickyPartitioner (Kafka 2.4+):
                 обирає одну партицію і "прилипає" до неї в межах batch,
                 потім переходить на наступну
```

### Чому це важливо

**Порядок гарантовано лише в межах партиції.**
Якщо всі події `user-42` в partition 1 — вони будуть оброблені строго по порядку.
Якщо events різних users розкидані по різних партиціях — це паралельна обробка.

```
Partition 0: [order-A user-99] [order-C user-99] [order-E user-99]
Partition 1: [order-B user-42] [order-D user-42] [order-F user-42]
Partition 2: [order-G user-17] [order-H user-17]

→ user-42 завжди: order-B → order-D → order-F (строгий порядок)
→ паралельна обробка між різними users
```

### branch02 vs branch03: чому orderId — поганий ключ

У branch02 ключем був `orderId` (UUID). UUID — рандомний:
- `order-a1b2c3` → partition 2
- `order-d4e5f6` → partition 0
- `order-g7h8i9` → partition 1

Якщо після `OrderCreated` надходить `OrderCancelled` для того самого замовлення,
вони можуть опинитися в різних партиціях → consumer обробить `Cancelled` РАНІШЕ `Created`.

З `userId` як ключем: обидві події одного user → та сама партиція → гарантований порядок.

---

## Як запустити

### Варіант A — тільки branch03

```bash
docker compose -f docker-compose-03.yml up --build
```

### Варіант B — разом з іншими гілками

Kafka і Kafka UI спільні. Сервіси мають унікальні порти:

```
branch01: order=8081, notification=8082
branch02: order=8091, notification=8092
branch03: order=8093, notification=8094
```

```bash
# Запустити kafka (якщо ще не запущена) через будь-який compose-файл
docker compose -f docker-compose-03.yml up --build
```

Перевірити:

```bash
docker compose -f docker-compose-03.yml ps
# kafka, kafka-ui, b03-order-service, b03-notification-service — Running/healthy
```

---

## Як протестувати

### 1. Демо з ключем: N замовлень одного userId → одна партиція

```bash
curl -s -X POST http://localhost:8093/api/orders/demo/keyed \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-42","count":6,"product":"Kafka Book"}' | jq
```

або у Windows OS:
```cmd
curl -s -X POST http://localhost:8093/api/orders/demo/keyed ^
  -H "Content-Type: application/json" ^
  -d "{\"userId\":\"user-42\",\"count\":6,\"product\":\"Kafka Book\"}"
```

Очікувана відповідь (всі записи в **одній** партиції):

```json
[
  {"orderId":"uuid-1","userId":"user-42","key":"user-42","partition":1,"offset":0},
  {"orderId":"uuid-2","userId":"user-42","key":"user-42","partition":1,"offset":1},
  {"orderId":"uuid-3","userId":"user-42","key":"user-42","partition":1,"offset":2},
  {"orderId":"uuid-4","userId":"user-42","key":"user-42","partition":1,"offset":3},
  {"orderId":"uuid-5","userId":"user-42","key":"user-42","partition":1,"offset":4},
  {"orderId":"uuid-6","userId":"user-42","key":"user-42","partition":1,"offset":5}
]
```

> Всі 6 повідомлень — `partition: 1`. Partition визначається один раз `hash("user-42") % 3 = 1` і не змінюється.

### 2. Демо без ключа: N замовлень → round-robin по партиціях

```bash
curl -s -X POST http://localhost:8093/api/orders/demo/round-robin \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-42","count":6,"product":"Kafka Book"}' | jq
```

або у Windows OS:
```cmd
curl -s -X POST http://localhost:8093/api/orders/demo/round-robin ^
  -H "Content-Type: application/json" ^
  -d "{\"userId\":\"user-42\",\"count\":6,\"product\":\"Kafka Book\"}"
```

Очікувана відповідь (розподіл по різних партиціях):

```json
[
  {"orderId":"uuid-1","userId":"user-42","key":null,"partition":0,"offset":0},
  {"orderId":"uuid-2","userId":"user-42","key":null,"partition":1,"offset":6},
  {"orderId":"uuid-3","userId":"user-42","key":null,"partition":2,"offset":0},
  {"orderId":"uuid-4","userId":"user-42","key":null,"partition":0,"offset":1},
  {"orderId":"uuid-5","userId":"user-42","key":null,"partition":1,"offset":7},
  {"orderId":"uuid-6","userId":"user-42","key":null,"partition":2,"offset":1}
]
```

> `key: null` → sticky partitioner розподіляє між 0, 1, 2. Порядок між повідомленнями НЕ гарантований.

### 3. Порівняти два різних userId

```bash
# user-42 → одна партиція (завжди та сама)
curl -s -X POST http://localhost:8093/api/orders/demo/keyed \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-42","count":3}' | jq '.[].partition'

# user-99 → інша партиція (але теж завжди та сама)
curl -s -X POST http://localhost:8093/api/orders/demo/keyed \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-99","count":3}' | jq '.[].partition'
```

Результат: `user-42` → всі `1`, `user-99` → всі `0` (або інша, але стабільна).

### 4. Звичайне замовлення (key = userId)

```bash
curl -s -X POST http://localhost:8093/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-42","product":"The Pragmatic Programmer","quantity":1,"totalAmount":45.00}' | jq
```

### 5. Логи notification-service (видно key)

```bash
docker logs b03-notification-service --tail=30
```

Очікуваний вивід:

```
╔══════════════════════════════════════╗
║  ORDER CREATED  #1
║  Key: user-42  →  Partition: 1  Offset: 0
║  Order ID  : f47ac10b-...
║  User ID   : user-42
║  Product   : Kafka Book #1 x1
║  Total     : $10.0
║  → Email sent to user user-42
╚══════════════════════════════════════╝
```

### 6. CLI-команди всередині контейнера

```bash
# Детальний опис топіку (партиції, реплікація, лідер)
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe --topic 03.orders.created

# Статус consumer group (offset, lag, partition assignment)
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group notification-service-group
```

Очікуваний вивід `--describe --topic 03.orders.created`:

```
Topic: 03.orders.created   PartitionCount: 3   ReplicationFactor: 1
  Topic: 03.orders.created  Partition: 0  Leader: 1  Replicas: 1  Isr: 1
  Topic: 03.orders.created  Partition: 1  Leader: 1  Replicas: 1  Isr: 1
  Topic: 03.orders.created  Partition: 2  Leader: 1  Replicas: 1  Isr: 1
```

### 7. Kafka UI — порівняти розподіл

Відкрити: [http://localhost:8080](http://localhost:8080)

- `Topics` → `03.orders.created` → `Messages`
- Колонка **Key** показує `user-42` (або `user-99`) для keyed messages, порожня для null-key
- Колонка **Partition** підтверджує: один key → одна partition

---

## Як це працює всередині

### OrderService.kt — що змінилося

#### branch02 (було)

```kotlin
// Ключ = orderId (UUID) — рандомний
kafkaTemplate.send(topicCreated, event.orderId, event)
kafkaTemplate.send(topicCancelled, orderId, event)
```

#### branch03 (стало)

```kotlin
// Ключ = userId — детермінований, однаковий для всіх замовлень того самого user
kafkaTemplate.send(topicCreated, userId, event)
// OrderCancelled теж йде в ту саму партицію що і OrderCreated для цього user
kafkaTemplate.send(topicCancelled, userId, event)
```

#### Нові batch-методи

```kotlin
fun createBatchKeyed(request: BatchOrderRequest): List<OrderSendResult>
// key = userId → всі повідомлення в одній партиції
// .get() — синхронне очікування → відповідь містить реальні partition + offset

fun createBatchNoKey(request: BatchOrderRequest): List<OrderSendResult>
// key = null → sticky partitioner розподіляє між партиціями
```

`kafkaTemplate.send(...).get()` — синхронне очікування підтвердження від брокера.
Це нормально для демо-endpoint-ів; у продакшені використовують async callbacks.

### OrderEventListener.kt — що змінилося

```kotlin
// branch02: лише partition і offset
log.info("║  Partition: {}  |  Offset: {}", record.partition(), record.offset())

// branch03: додано key
log.info("║  Key: {}  →  Partition: {}  Offset: {}",
    record.key(), record.partition(), record.offset())
```

`record.key()` — повертає рядок `"user-42"` або `null` залежно від того, чи був key при відправці.

---

## Топіки та їх налаштування

| Топік | Партиції | Retention | Призначення |
|-------|----------|-----------|-------------|
| `03.orders.created` | **3** | 7 днів | Нові замовлення, key=userId |
| `03.orders.cancelled` | 1 | 7 днів | Скасування, key=userId |
| `03.payments.processed` | **3** | 7 днів | Резерв для branch04+ |
| `03.notifications.sent` | 1 | 1 день | Резерв для branch04+ |

---

## Структура проєкту (зміни відносно branch02)

```
kafka-laboratory/
├── docker-compose-03.yml                   ← container names b03-*, порти 8093/8094
├── branch03_keys/
│   ├── order-service/
│   │   └── src/main/kotlin/com/kafkalab/order/
│   │       ├── controller/OrderController.kt  ← нові POST /demo/keyed, /demo/round-robin
│   │       ├── model/
│   │       │   ├── BatchOrderRequest.kt        ← NEW: {userId, count, product}
│   │       │   └── OrderSendResult.kt          ← NEW: {orderId, userId, key, partition, offset}
│   │       └── service/OrderService.kt         ← key змінено на userId, + createBatchKeyed/NoKey
│   │   └── src/main/resources/
│   │       └── application.yml                ← port 8093
│   └── notification-service/
│       └── src/main/kotlin/com/kafkalab/notification/
│           └── listener/OrderEventListener.kt  ← виводить record.key() у логах
│       └── src/main/resources/
│           └── application.yml                ← port 8094
```

---

## Ключові концепції цієї гілки

| Концепція | Що демонструє приклад |
|-----------|----------------------|
| **Key → Partition** | `hash("user-42") % 3` завжди дає ту саму партицію |
| **Order guarantee** | Всі повідомлення одного userId в одній партиції → строгий порядок |
| **null key** | `demo/round-robin` → повідомлення розподіляються між partitions 0,1,2 |
| **Sticky Partitioner** | null-key повідомлення "прилипають" до партиції в межах batch (Kafka 2.4+) |
| **Determinism** | Той самий userId → та сама партиція навіть після рестарту |
| **Consumer sees key** | `record.key()` у listener дозволяє бачити routing-рішення producer-а |
| **Correct key choice** | `userId` гарантує: Created і Cancelled одного user → одна партиція → правильний порядок |

---

## Що далі — branch04

У наступній гілці вивчаємо Consumer Groups:
- Запустимо 3 екземпляри notification-service в одній group
- Кожен отримає 1 партицію з `03.orders.created` (3 партиції / 3 consumers = 1:1)
- Зупинимо один екземпляр → rebalance → 2 consumers читають по 1-2 партиції
- Partition assignment strategies: `RangeAssignor` vs `RoundRobinAssignor`