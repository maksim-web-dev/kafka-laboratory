# Branch 03 — Message Keys

---

## `branch03_keys` — що вивчаємо

- Ключ повідомлення (`message key`) — як producer призначає ключ і чому це важливо
- Алгоритм вибору партиції: `hash(key) % numPartitions` (Murmur2)
- Детермінованість: один і той самий ключ → завжди та сама партиція
- Гарантія порядку: в межах однієї партиції повідомлення читаються суворо по черзі
- `null` ключ → StickyPartitioner: повідомлення розподіляються між партиціями без гарантії порядку
- Практична різниця між `orderId` (UUID, рандомний) і `userId` (детермінований) як ключами
- Нові batch-endpoints для порівняння keyed vs non-keyed поведінки
- `record.key()` у consumer — як переглянути routing-рішення producer-а
- `kafkaTemplate.send(...).get()` — синхронне підтвердження для демо-endpoint-ів
- Правило вибору ключа: коли важливий порядок подій одного entity → ключ = ідентифікатор entity

---

## Що змінилося порівняно з branch02

- Ключ `03.orders.created` та `03.orders.cancelled`:
  `orderId` (UUID, рандомний) → **`userId`** (детермінований)
- Нові batch-endpoint-и:
  `POST /api/orders/demo/keyed` та `POST /api/orders/demo/round-robin`
- Нові моделі:
  `BatchOrderRequest` (userId, count, product) та
  `OrderSendResult` (orderId, userId, key, partition, offset)
- Відповідь batch-демо містить реальний `partition` і `offset` з брокера (синхронний `.get()`)
- Лог notification-service: тепер виводить `Key: user-42  →  Partition: X  Offset: Y`
  (раніше лише Partition і Offset)

---

## Архітектура

```
POST /api/orders              key = userId  ─┐
POST /api/orders/demo/keyed   key = userId  ─┤─── 03.orders.created (3 partitions)
POST /api/orders/demo/round-robin  key=null ─┘

hash(userId) % 3 → partition 0, 1, або 2

user-42  → hash("user-42") % 3 = 1  → partition 1  (завжди!)
user-99  → hash("user-99") % 3 = 0  → partition 0  (завжди!)
null key →  sticky partitioner      → round-robin   (непередбачувано)

┌─────────────────┐   03.orders.created (3 partitions)   ┌───────────────────────┐
│  order-service  │  ──────────────────────────────────▶ │                       │
│  :8081          │                                       │  notification-service │
│                 │   03.orders.cancelled (1 partition)   │  :8082                │
│                 │  ──────────────────────────────────▶ │                       │
└─────────────────┘                                       └───────────────────────┘
         │                                                          │
         └─────────────────────────┬────────────────────────────────┘
                                   │
                      ┌────────────▼────────────┐
                      │      Apache Kafka        │
                      │      kafka:9092          │
                      │      (KRaft mode)        │
                      └────────────┬────────────┘
                                   │
                      ┌────────────▼────────────┐
                      │      Kafka UI            │
                      │      :8080               │
                      └─────────────────────────┘
```

### Топіки та їх налаштування

- **`03.orders.created`** — 3 партиції, retention 7 днів — нові замовлення, key=userId
- **`03.orders.cancelled`** — 1 партиція, retention 7 днів — скасування, key=userId, строгий порядок
- **`03.payments.processed`** — 3 партиції, retention 7 днів — резерв для branch04+
- **`03.notifications.sent`** — 1 партиція, retention 1 день — резерв для branch04+

> Префікс `03.` відокремлює топіки цієї гілки від інших при роботі в одному Kafka кластері.

---

## Ключові концепції цієї гілки

### Як key визначає партицію

Kafka використовує алгоритм Murmur2 для хешування ключа:

```
partition = Math.abs(murmur2(key.getBytes())) % numPartitions

key="user-42"  → murmur2 → 0x7f3a1b2c → abs % 3 = 1
key="user-42"  → той самий хеш → та сама партиція → ЗАВЖДИ partition 1
key="user-99"  → murmur2 → інше значення → abs % 3 = 0
```

**Детермінованість**: якщо ключ не змінюється і кількість партицій не змінюється — routing стабільний.
Навіть після рестарту брокера чи producer-а `user-42` завжди потрапить у partition 1.

### Гарантія порядку в межах партиції

Kafka гарантує порядок лише **в межах однієї партиції**.
Якщо всі події одного `userId` потрапляють в одну партицію — consumer обробить їх строго по черзі:

```
Partition 0: [order-A user-99] [order-C user-99] [order-E user-99]
Partition 1: [order-B user-42] [order-D user-42] [order-F user-42]
Partition 2: [order-G user-17] [order-H user-17]

→ user-42 завжди: order-B → order-D → order-F (строгий порядок)
→ між різними users: паралельна обробка без гарантій порядку
```

### Чому orderId — поганий ключ

На перший погляд здається, що `orderId` — нормальний вибір:
`OrderCreated` і `OrderCancelled` для одного замовлення мають **однаковий** `orderId`,
тому `hash(orderId) % 3` дасть ту саму партицію для обох.
Порядок для конкретного замовлення буде збережений.

Проблема виникає коли **один користувач робить кілька замовлень**:

```
OrderCreated(orderId=order-A, userId=user-42) → hash("order-A") % 3 = 2  → partition 2
OrderCreated(orderId=order-B, userId=user-42) → hash("order-B") % 3 = 0  → partition 0
OrderCancelled(orderId=order-A, userId=user-42) → hash("order-A") % 3 = 2 → partition 2
```

`order-A` і `order-B` — різні UUID, різний хеш → різні партиції.
Якщо бізнес-логіка має залежність між замовленнями одного user-а
(кредитний ліміт, saga-компенсація, програма лояльності) —
порядок між `order-A` і `order-B` не гарантований.

З `userId` як ключем — усі події `user-42` незалежно від замовлення → **одна партиція**:

```
OrderCreated(order-A, user-42) → hash("user-42") % 3 = 1 → partition 1
OrderCreated(order-B, user-42) → hash("user-42") % 3 = 1 → partition 1
OrderCancelled(order-A, user-42) → hash("user-42") % 3 = 1 → partition 1
```

У multi-consumer сценарії (branch04+) це додатково означає,
що всі події `user-42` завжди обробляє **один і той самий consumer-екземпляр** —
без race condition між instances.

### null key → StickyPartitioner

Коли key=null, Kafka 2.4+ використовує `StickyPartitioner`:
- Обирає одну партицію і "прилипає" до неї в межах одного batch
- Після відправки batch переходить на наступну партицію
- Це краще, ніж round-robin (більший batch → кращий throughput),
  але порядок між повідомленнями НЕ гарантований

---

## Як запустити

```bash
docker compose -f docker-compose-03.yml up --build
```

Перевірити готовність:

```bash
docker compose -f docker-compose-03.yml ps
# kafka, kafka-ui, b03-order-service, b03-notification-service — Running/healthy
```

---

## Як протестувати

### 1. Демо з ключем: N замовлень одного userId → одна партиція

```bash
curl -s -X POST http://localhost:8081/api/orders/demo/keyed \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-42","count":6,"product":"Kafka Book"}' | jq
```

або у Windows OS:
```cmd
curl -s -X POST http://localhost:8081/api/orders/demo/keyed ^
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

> Всі 6 повідомлень — `partition: 1`.
> Значення partition визначається один раз: `hash("user-42") % 3 = 1`.

### 2. Демо без ключа: N замовлень → round-robin по партиціях

```bash
curl -s -X POST http://localhost:8081/api/orders/demo/round-robin \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-42","count":6,"product":"Kafka Book"}' | jq
```

або у Windows OS:
```cmd
curl -s -X POST http://localhost:8081/api/orders/demo/round-robin ^
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

> `key: null` → sticky partitioner розподіляє між 0, 1, 2.
> Порядок між повідомленнями НЕ гарантований.

### 3. Порівняти два різних userId

```bash
# user-42 → одна партиція (завжди та сама)
curl -s -X POST http://localhost:8081/api/orders/demo/keyed \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-42","count":3}' | jq '.[].partition'

# user-99 → інша партиція (але теж завжди та сама)
curl -s -X POST http://localhost:8081/api/orders/demo/keyed \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-99","count":3}' | jq '.[].partition'
```

Результат: `user-42` → всі `1`, `user-99` → всі `0` (або інша, але стабільна).

### 4. Звичайне замовлення (key = userId)

```bash
curl -s -X POST http://localhost:8081/api/orders \
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
- Колонка **Key** показує `user-42` для keyed-повідомлень, порожня для null-key
- Колонка **Partition** підтверджує: один key → одна partition

---

## Як це працює всередині

### Producer (order-service) — що змінилося

#### branch02 (було)

```kotlin
// Ключ = orderId (UUID) — рандомний, не дає гарантії порядку
kafkaTemplate.send(topicCreated, event.orderId, event)
kafkaTemplate.send(topicCancelled, orderId, event)
```

#### branch03 (стало)

```kotlin
// Ключ = userId — детермінований, однаковий для всіх замовлень того самого user
kafkaTemplate.send(topicCreated, userId, event)
// OrderCancelled теж іде в ту саму партицію що і OrderCreated для цього userId
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

> `kafkaTemplate.send(...).get()` — синхронне очікування підтвердження від брокера.
> Це зроблено лише для демо-endpoint-ів щоб показати реальні значення partition/offset у відповіді.
> У продакшені використовують async callbacks.

### Consumer (notification-service) — що змінилося

#### OrderEventListener.kt

```kotlin
// branch02: лише partition і offset
log.info("║  Partition: {}  |  Offset: {}", record.partition(), record.offset())

// branch03: додано key
log.info("║  Key: {}  →  Partition: {}  Offset: {}",
    record.key(), record.partition(), record.offset())
```

`record.key()` повертає рядок `"user-42"` або `null` залежно від того, чи був key при відправці.

---

## Структура проєкту (зміни відносно branch02)

```
kafka-laboratory/
├── docker-compose-03.yml                   ← порти 8081/8082
├── branch03_keys/
│   ├── order-service/
│   │   └── src/main/kotlin/com/kafkalab/order/
│   │       ├── controller/OrderController.kt  ← нові POST /demo/keyed, /demo/round-robin
│   │       ├── model/
│   │       │   ├── BatchOrderRequest.kt        ← NEW: {userId, count, product}
│   │       │   └── OrderSendResult.kt          ← NEW: {orderId, userId, key, partition, offset}
│   │       └── service/OrderService.kt         ← key змінено на userId, + createBatchKeyed/NoKey
│   │   └── src/main/resources/
│   │       └── application.yml                ← port 8081
│   └── notification-service/
│       └── src/main/kotlin/com/kafkalab/notification/
│           └── listener/OrderEventListener.kt  ← виводить record.key() у логах
│       └── src/main/resources/
│           └── application.yml                ← port 8082
```

---

## Що далі — branch04

У наступній гілці вивчаємо **Consumer Groups**:
- Запустимо 3 екземпляри notification-service в одній consumer group
- Кожен отримає 1 партицію з `03.orders.created` (3 партиції / 3 consumers = 1:1)
- Зупинимо один екземпляр → автоматичний rebalance → 2 consumers читають по 1-2 партиції
- Partition assignment strategies: `RangeAssignor` vs `RoundRobinAssignor`

---
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------

## Слайди для презентації

**Слайд 1: Заголовок**
- Branch 03 — Message Keys
- Як Kafka визначає, в яку партицію відправити повідомлення

---

**Слайд 2: Проблема з orderId як ключем**
- У branch02 ключ = orderId (UUID) — унікальний для кожного замовлення
- Різні замовлення одного user-а → різні партиції
- У multi-consumer сценарії: events user-а обробляють різні instances
- Порядок між замовленнями не гарантований → бізнес-логіка може зламатись

---

**Слайд 3: Алгоритм вибору партиції**
- Kafka DefaultPartitioner → Murmur2 hash
- `partition = Math.abs(murmur2(key.getBytes())) % numPartitions`
- Той самий ключ → той самий хеш → та сама партиція
- Детермінованість: не залежить від часу, рестарту, порядку відправки

---

**Слайд 4: userId як правильний ключ**
- user-42 → hash % 3 = 1 → partition 1 (ЗАВЖДИ)
- user-99 → hash % 3 = 0 → partition 0 (ЗАВЖДИ)
- Всі події одного користувача → одна партиція
- У multi-consumer: всі події user-42 → один і той самий consumer-instance

---

**Слайд 5: Гарантія порядку**
- Kafka гарантує порядок ТІЛЬКИ в межах однієї партиції
- Між партиціями — порядок не гарантований
- Висновок: key = entity-ідентифікатор → порядок подій гарантований
- Правило: якщо важливий порядок подій → ключ = ідентифікатор entity

---

**Слайд 6: null key → StickyPartitioner**
- Коли key = null → StickyPartitioner (Kafka 2.4+)
- "Прилипає" до однієї партиції в межах batch
- Потім переходить на наступну партицію
- Порядок між повідомленнями НЕ гарантований

---

**Слайд 7: Паралелізм vs Порядок**
- 3 партиції → до 3 consumers паралельно
- Чим більше партицій → вищий throughput
- Але: більше партицій → слабші гарантії порядку між entities
- Компроміс: вибирати кількість партицій під очікуване навантаження + consumer count

---

**Слайд 8: Демо — keyed batch**
- `POST /api/orders/demo/keyed` → userId=user-42, count=6
- Відповідь: всі 6 повідомлень у partition=1
- Kafka UI: колонка Key показує "user-42", Partition = 1

---

**Слайд 9: Демо — round-robin batch**
- `POST /api/orders/demo/round-robin` → key=null, count=6
- Відповідь: повідомлення у partitions 0, 1, 2 (нерівномірно)
- Kafka UI: Key порожній, Partition різні

---

**Слайд 10: record.key() у Consumer**
- `@KafkaListener` → `ConsumerRecord<String, OrderCreatedEvent>`
- `record.key()` → повертає рядок "user-42" або null
- Лог: `Key: user-42  →  Partition: 1  Offset: 0`
- Consumer бачить routing-рішення producer-а

---

**Слайд 11: Ключові висновки**
- Message key → детермінований вибір партиції (Murmur2 hash)
- userId > orderId як ключ: всі події одного user → одна партиція → один consumer-instance
- null key → sticky partitioner → рівномірний розподіл, але без порядку
- Порядок гарантований ТІЛЬКИ в межах партиції

---

**Слайд 12: Що далі — Consumer Groups**
- branch04: Consumer Groups
- 3 instances notification-service → кожен читає 1 партицію
- Зупинити один → rebalance → перерозподіл партицій
- RangeAssignor vs RoundRobinAssignor

---

## Текст для презентації (скрипт)

**Слайд 1:**
Вітаю! Сьогодні ми розглянемо одну з найважливіших концепцій Kafka — message keys.
На перший погляд це проста ідея, але вона фундаментально визначає,
як ваша система буде обробляти порядок подій.
Ми побачимо, чому вибір ключа може або зламати, або гарантувати правильність бізнес-логіки.

**Слайд 2:**
У branch02 ми використовували `orderId` як ключ повідомлення.
OrderId — це UUID, унікальний для кожного замовлення.
Це означає, що різні замовлення одного користувача потрапляють у різні партиції.
Якщо в системі є залежність між замовленнями — наприклад, кредитний ліміт або saga-компенсація —
порядок між ними не гарантований.
Це класична проблема, яка зустрічається в реальних системах.

**Слайд 3:**
Як Kafka вирішує, в яку партицію відправити повідомлення?
За допомогою алгоритму Murmur2.
Формула: partition = абсолютне значення хешу ключа, поділене по модулю на кількість партицій.
Ключова властивість — детермінованість:
той самий рядок завжди дає той самий хеш, а отже завжди потрапляє в ту саму партицію.

**Слайд 4:**
Замінимо ключ з orderId на userId.
Тепер user-42 завжди потрапляє в partition 1, user-99 — в partition 0.
Всі події одного користувача збираються в одній партиції.
Коли в branch04 ми запустимо кілька consumer-instances,
всі події user-42 завжди оброблятиме один і той самий instance — без race condition.

**Слайд 5:**
Важливо розуміти: Kafka гарантує порядок ТІЛЬКИ в межах однієї партиції.
Між різними партиціями — порядок не гарантований.
Саме тому вибір ключа так важливий.
Загальне правило: якщо вам важливий порядок подій для якогось entity —
використовуйте ідентифікатор цього entity як ключ.

**Слайд 6:**
А що трапляється, якщо ключ не вказаний?
Kafka 2.4 ввів StickyPartitioner.
Він обирає одну партицію і "прилипає" до неї протягом відправки batch.
Це краще, ніж старий round-robin, тому що більший batch = менше системних викликів = кращий throughput.
Але порядок між повідомленнями все одно не гарантований.

**Слайд 7:**
Тут є важливий компроміс.
Більше партицій = більший паралелізм = вищий throughput.
Але більше партицій також означає, що різні events одного entity можуть читатися різними consumers.
Тому рекомендація: вибирайте кількість партицій виходячи з очікуваного навантаження
і майбутньої кількості consumers у групі.

**Слайд 8:**
Давайте подивимось на практику.
Endpoint `/api/orders/demo/keyed` дозволяє надіслати N замовлень з одним userId.
Відповідь повертає partition і offset для кожного повідомлення.
Як бачите — всі 6 повідомлень у partition 1.
Kafka UI підтверджує: колонка Key показує "user-42", Partition = 1 для всіх.

**Слайд 9:**
Другий endpoint — `/api/orders/demo/round-robin` — надсилає ті ж повідомлення але без ключа.
Тепер partition різні: 0, 1, 2 в різному порядку.
Kafka UI показує порожню колонку Key.
Той самий userId, але без ключа — і порядок вже не гарантований.

**Слайд 10:**
На стороні consumer ми додали вивід ключа у логи:
`record.key()` повертає рядок "user-42" або null.
Це важливо для дебагінгу — ви завжди можете перевірити, який routing-ключ використовував producer.
В Kafka UI теж видно ключ у колонці Key в розділі Messages.

**Слайд 11:**
Підсумуємо.
По-перше: message key визначає партицію через Murmur2 hash, і це детерміновано.
По-друге: userId краще за orderId як ключ —
всі події одного користувача в одній партиції, у одного consumer-instance.
По-третє: null key дає sticky partitioner — рівномірний розподіл, але без гарантій порядку.
І головне правило: порядок гарантований ТІЛЬКИ в межах однієї партиції.

**Слайд 12:**
У наступній гілці, branch04, ми переходимо до consumer groups.
Ми запустимо три екземпляри notification-service і побачимо,
як Kafka автоматично розподіляє партиції між consumers.
Зупинимо один екземпляр і спостерігатимемо rebalance —
коли решта consumers перебирають осиротілі партиції. До зустрічі!

---

## Тестові питання

**Питання 1:** Який алгоритм Kafka використовує для визначення партиції за ключем?

A) SHA-256
B) Murmur2
C) MD5
D) CRC32

**Відповідь:** B — Kafka використовує алгоритм Murmur2 для хешування ключа.
Формула: `partition = Math.abs(murmur2(key.getBytes())) % numPartitions`.

---

**Питання 2:** Що станеться, якщо надіслати два повідомлення з однаковим ключем `userId="user-42"`
у топік з 3 партиціями?

A) Вони завжди потраплять в одну і ту ж партицію
B) Вони розподіляться між партиціями рандомно
C) Kafka відхилить друге повідомлення як дублікат
D) Вони обидва потраплять у partition 0

**Відповідь:** A — Той самий ключ завжди дає той самий хеш → та сама партиція.
Це детерміновано та не залежить від часу відправки.

---

**Питання 3:** У якому випадку Kafka гарантує порядок обробки повідомлень?

A) Тільки якщо є один consumer у групі
B) Тільки в межах однієї партиції
C) Для всіх повідомлень одного топіку
D) Тільки якщо ключ не null

**Відповідь:** B — Kafka гарантує порядок виключно в межах однієї партиції.
Між різними партиціями — порядок не гарантований.

---

**Питання 4:** Чому `userId` є кращим ключем, ніж `orderId` (UUID)?

A) UUID довший і займає більше місця
B) userId — детермінований: всі події одного користувача потрапляють в одну партицію,
   що гарантує порядок між усіма його замовленнями
C) orderId не підтримується Kafka
D) UUID не можна хешувати алгоритмом Murmur2

**Відповідь:** B — orderId унікальний для кожного замовлення, тому різні замовлення одного user-а
розкидаються по різних партиціях.
З userId усі події user-а в одній партиції → гарантований порядок між замовленнями.

---

**Питання 5:** Що таке StickyPartitioner і коли він використовується?

A) Алгоритм для хешування ключів з спеціальними символами
B) Стратегія для повідомлень з null-ключем: "прилипає" до однієї партиції в межах batch
C) Спосіб закріпити partitioner до конкретного consumer
D) Метод рівномірного розподілу повідомлень між топіками

**Відповідь:** B — StickyPartitioner обирає одну партицію і відправляє до неї всі повідомлення одного batch.
Потім переходить на наступну.
Це ефективніше за round-robin, але порядок між batch не гарантований.

---

**Питання 6:** Скільки партицій отримає consumer group з 1 consumer при топіку з 3 партиціями?

A) 1
B) 3
C) 0 (consumer не може читати більше однієї)
D) Залежить від assignor

**Відповідь:** B — Якщо в consumer group є 1 consumer, він читає всі 3 партиції сам.
Kafka призначає кожному consumer максимально можливу кількість партицій.

---

**Питання 7:** Що повертає `record.key()` у `@KafkaListener` для повідомлення надісланого без ключа?

A) Порожній рядок `""`
B) `null`
C) UUID, згенерований Kafka
D) Назву топіку

**Відповідь:** B — Якщо producer не вказав ключ, `record.key()` повертає `null`.

---

**Питання 8:** Яка формула визначає, в яку партицію потрапить повідомлення з ключем?

A) `ordinal(key) % numPartitions`
B) `hash(key) + numPartitions`
C) `Math.abs(murmur2(key.getBytes())) % numPartitions`
D) `key.hashCode() % numPartitions`

**Відповідь:** C — Kafka використовує Murmur2 hash, бере абсолютне значення
і ділить по модулю на кількість партицій.

---

**Питання 9:** Що відбудеться, якщо збільшити кількість партицій топіку з 3 до 6
після того, як дані вже записані?

A) Нічого не зміниться, старі повідомлення залишаться на місці
B) Нові повідомлення з тим самим ключем можуть потрапити в ІНШУ партицію
C) Kafka автоматично перерозподілить всі старі повідомлення
D) Це заборонено в Kafka

**Відповідь:** B — `hash("user-42") % 3 = 1`, але `hash("user-42") % 6 = 4`.
Зміна кількості партицій змінює routing і порушує гарантію порядку для вже накопичених даних.
Тому кількість партицій треба планувати заздалегідь.

---

**Питання 10:** Для яких use-cases найкраще підходить null key?

A) Коли важливий строгий порядок обробки
B) Для рівномірного завантаження всіх партицій без потреби в упорядкуванні
C) Коли consumer group має лише один екземпляр
D) Для exactly-once semantics

**Відповідь:** B — null key (з StickyPartitioner) добре підходить для випадків,
коли порядок не важливий, але потрібен рівномірний розподіл навантаження між партиціями
(наприклад, log-повідомлення, метрики).

---

**Питання 11:** Користувач `user-42` має два замовлення. Яку проблему спричинить `orderId` як ключ
у multi-consumer сценарії?

```
OrderCreated (orderId=order-A, userId=user-42) → partition 2
OrderCreated (orderId=order-B, userId=user-42) → partition 0
OrderCancelled (orderId=order-A, userId=user-42) → partition 2
```

A) OrderCreated і OrderCancelled для order-A потраплять у різні партиції
B) Events різних замовлень одного user-а розкидані по різних партиціях —
   у multi-consumer сценарії вони обробляються різними instances без гарантії порядку між ними
C) orderId не можна використовувати як ключ у Kafka
D) Підвищений latency через рандомний UUID

**Відповідь:** B — OrderCreated і OrderCancelled **одного** замовлення (orderId однаковий) —
в одній партиції, це нормально.
Проблема інша: `order-A` і `order-B` мають різні UUID → різні партиції → різні consumer-instances.
Якщо є залежність між замовленнями одного user-а, порядок не гарантований.
З `userId` як ключем усі events `user-42` завжди в одній партиції та у одного consumer.

---

**Питання 12:** Що таке детермінованість routing у Kafka?

A) Kafka завжди відправляє нові повідомлення в partition 0
B) Той самий ключ завжди дає ту саму партицію, незалежно від часу та кількості спроб
C) Kafka рівномірно розподіляє ключі між партиціями
D) Routing визначається на стороні consumer, а не producer

**Відповідь:** B — Murmur2 — детермінована функція: для одного і того самого input
вона завжди дає той самий output.
Це означає, що `user-42` завжди буде в одній і тій самій партиції, навіть після рестарту системи.

---

**Питання 13:** Яке твердження щодо паралелізму та порядку в Kafka є правильним?

A) Більше партицій = кращий порядок
B) Менше партицій = вищий throughput
C) Більше партицій = вищий throughput, але слабші гарантії порядку між entities
D) Порядок і throughput не пов'язані

**Відповідь:** C — Партиції — це одиниця паралелізму.
Більше партицій = більше consumers можуть читати паралельно = вищий throughput.
Але при цьому різні events одного entity можуть оброблятись різними consumers незалежно.

---

**Питання 14:** Яка різниця між `kafkaTemplate.send(...).get()` і звичайним async `send()`?

A) `.get()` — синхронне очікування результату (partition, offset); async — вогонь і забудь
B) `.get()` відправляє повідомлення двічі для надійності
C) Async відправляє повідомлення одночасно в усі партиції
D) `.get()` доступний тільки для consumer, не producer

**Відповідь:** A — `.get()` блокує потік до отримання підтвердження від брокера.
Це дозволяє повернути реальні значення partition і offset.
У продакшені зазвичай використовують async з callback щоб не блокувати обробку.

---

**Питання 15:** Producer надсилає `OrderCreatedEvent` з `key="user-42"`. Топік має 3 партиції.
Через 5 хвилин producer перезапустили. Куди потрапить наступне повідомлення з `key="user-42"`?

A) В partition 0 (починає з початку після рестарту)
B) В ту саму partition що і раніше (partition 1)
C) Kafka не може відповісти на це питання без контексту
D) Рандомно, як після рестарту consumer

**Відповідь:** B — Murmur2 хеш детермінований і не залежить від стану producer.
`hash("user-42") % 3` дасть той самий результат завжди.

---

**Питання 16:** Чому в Kafka UI колонка Key порожня для повідомлень з `key=null`?

A) Kafka UI не підтримує відображення null-ключів
B) Kafka зберігає null як порожній рядок, але UI відображає його як порожнє поле
C) Повідомлення без ключа мають порожню колонку Key, оскільки ключ відсутній у метаданих
D) Це баг у Kafka UI версії 0.7+

**Відповідь:** C — Якщо key не вказаний, він зберігається як null у метаданих повідомлення.
Kafka UI відображає null як порожнє поле.

---

**Питання 17:** Що відбудеться при `RoundRobinAssignor` у consumer group з 2 consumers
і топіком з 3 партиціями?

A) Consumer 1 отримає 3 партиції, Consumer 2 — 0
B) Consumer 1 отримає partitions 0, 2; Consumer 2 — partition 1
C) Consumer 1 отримає partitions 0, 1; Consumer 2 — partition 2
D) Обидва consumers читають всі 3 партиції одночасно

**Відповідь:** B — RoundRobinAssignor розподіляє партиції по черзі: p0→C1, p1→C2, p2→C1.
Результат: C1 отримує 0 і 2, C2 — тільки 1.

---

**Питання 18:** Який сценарій ПРАВИЛЬНО демонструє використання message key?

A) Надсилати всі payment events з key=`"payment"` в один топік
B) Надсилати account events з key=`accountId` щоб всі зміни одного рахунку оброблялись по порядку
C) Надсилати log-повідомлення з key=timestamp для хронологічного порядку
D) Надсилати всі events з key=`null` для рівномірного розподілу

**Відповідь:** B — Ключ = ідентифікатор entity (accountId) гарантує,
що всі зміни одного рахунку потраплять в одну партицію і будуть оброблені в правильному порядку.

---

**Питання 19:** Consumer group читає топік з 3 партиціями.
Всі повідомлення мають key=`"fixed-key"`. Яка проблема виникне?

A) Kafka не дозволяє фіксований ключ
B) Всі повідомлення потраплять в одну партицію → тільки один consumer буде завантажений,
   решта простоюватимуть
C) Повідомлення будуть дубльовані між партиціями
D) Throughput збільшиться бо хешування стабільне

**Відповідь:** B — Якщо всі ключі однакові → всі повідомлення в одній партиції →
одна partition читається одним consumer.
Решта consumers у групі простоюють. Це anti-pattern — hot partition.

---

**Питання 20:** Для CCDAK іспиту: яке твердження правильне щодо Kafka Default Partitioner?

A) Він використовує round-robin для ключів і StickyPartitioner для null
B) Він використовує Murmur2 hash для ключів і StickyPartitioner для null-ключів (Kafka 2.4+)
C) Він завжди використовує round-robin незалежно від наявності ключа
D) Він використовує Java `hashCode()` для визначення партиції

**Відповідь:** B — Це класичне питання CCDAK.
DefaultPartitioner: якщо ключ є → Murmur2 hash;
якщо ключ null → StickyPartitioner (починаючи з Kafka 2.4, раніше був round-robin).