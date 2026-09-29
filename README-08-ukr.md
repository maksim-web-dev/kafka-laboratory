# Branch 08 — Consumer Configuration

## `branch08_consumer_configuration` — що вивчаємо

- **CooperativeStickyAssignor** — incremental rebalance: перерозподіляються лише "переїжджаючі" партиції.
- **EAGER vs COOPERATIVE** — різниця між stop-the-world і поступовим rebalance.
- **Static Group Membership** — `group.instance.id` для зменшення кількості rebalance при рестарті.
- **`onPartitionsRevoked` у COOPERATIVE** — викликається лише для партицій що звільняються.
- **`max.poll.records`** — обмеження кількості записів за один `poll()`.
- **`max.poll.interval.ms`** — максимальний час між двома `poll()` до kick-out з групи.
- **`session.timeout.ms`** — час без heartbeat після якого broker визнає consumer dead.
- **`heartbeat.interval.ms`** — частота відправки heartbeat (правило: < session.timeout / 3).
- **Graceful shutdown** — `server.shutdown=graceful` + commit перед виходом.
- **`/api/notifications/partitions`** — новий ендпоінт для перегляду призначених партицій.

## Що змінилося порівняно з branch07

- Повернуто 2 екземпляри `notification-service` (1-b08 на 8082, 2-b08 на 8083).
- `partition.assignment.strategy` змінено на `CooperativeStickyAssignor`.
- Налаштовано `group.instance.id` для кожного `@KafkaListener` — static membership.
- Додано `max.poll.records=10`, `max.poll.interval.ms=300000`, `session.timeout.ms=45000`.
- `server.shutdown=graceful` та `lifecycle.timeout-per-shutdown-phase=30s`.
- Новий ендпоінт `/api/notifications/partitions` — показує призначені партиції.
- Новий ендпоінт `/api/notifications/config` — показує поточну конфігурацію consumer.
- Назви топіків змінено на `08.orders.*`.

## Архітектура

```
order-service-b08 :8081
  POST /api/orders → kafkaTemplate.send(topicCreated, userId, event)
          │
     08.orders.created (3 partitions)
          │
    ┌─────┴────────────────────────────────────┐
    │ notification-service-group (COOPERATIVE)  │
    │                                           │
    │  notification-1-b08 :8082                 │
    │    group.instance.id:                     │
    │      "notification-1-orders-created"      │
    │    assigned: [partition 0, partition 1]   │
    │                                           │
    │  notification-2-b08 :8083                 │
    │    group.instance.id:                     │
    │      "notification-2-orders-created"      │
    │    assigned: [partition 2]                │
    └───────────────────────────────────────────┘

COOPERATIVE rebalance (коли notification-2 виходить):
  REVOKED:  notification-2 → [partition 2]        ← тільки ця партиція
  ASSIGNED: notification-1 → [partition 2]        ← notification-1 продовжує [0,1] без перерви
  (НЕ торкаємось partition 0 і 1 — вони залишаються у notification-1)
```

### Топіки та їх налаштування

- `08.orders.created` → партиції: 3, retention: 7 днів,
  читається `notification-service-group` з CooperativeStickyAssignor.
- `08.orders.cancelled` → партиції: 1, retention: 7 днів.

## Ключові концепції цієї гілки

### EAGER vs COOPERATIVE Rebalance

**EAGER (RangeAssignor — branch04):**

```
Event: notification-2 приєднується до групи
  1. ВСІ consumers: REVOKED усі партиції → зупиняють читання
  2. Group coordinator: перераховує призначення
  3. ВСІ consumers: ASSIGNED нові партиції → відновлюють читання

Stop-the-world: увесь consumer group не читає протягом rebalance (секунди)
```

**COOPERATIVE (CooperativeStickyAssignor — branch08):**

```
Event: notification-2 приєднується до групи
  Round 1:
    consumers повідомляють що мають: notification-1 → [0,1,2]
    coordinator визначає: partition 2 перейде до notification-2
    notification-1 REVOKED: лише [partition 2] ← не [0] і не [1]
  Round 2:
    notification-2 ASSIGNED: [partition 2]
    notification-1 продовжує читати [0,1] без жодної перерви ✓
```

Логи при COOPERATIVE:

```
# EAGER (branch04):
[REBALANCE] REVOKED:  [orders.created[0], orders.created[1], orders.created[2]]
[REBALANCE] ASSIGNED: [orders.created[0], orders.created[1]]

# COOPERATIVE (branch08):
[COOPERATIVE] notification-1 REVOKED:  [orders.created[2]]   ← лише ця
[COOPERATIVE] notification-2 ASSIGNED: [orders.created[2]]
```

### Static Group Membership

Без `group.instance.id` (dynamic): кожен рестарт → 2 rebalance (disconnect + reconnect).

```
Dynamic membership:
  notification-2 перезапускається
  → broker: "member leaves" → rebalance (partition 2 переходить до notification-1)
  → broker: "new member joins" → rebalance знову (partition 2 повертається)
  = 2 rebalance, ~session.timeout.ms downtime

Static membership (group.instance.id):
  notification-2 перезапускається
  → broker: "instance notification-2-orders-created offline"
  → broker чекає session.timeout.ms (45с) перш ніж declared dead
  → якщо notification-2 повернувся ДО session.timeout → NO rebalance ✓
  = 0 rebalance при швидкому рестарті
```

Кожен `@KafkaListener` отримує унікальний instance ID:

```kotlin
// application.yml
instance:
  id: ${INSTANCE_ID:notification-1}

// OrderEventListener.kt
@KafkaListener(
    topics = ["08.orders.created"],
    groupId = "notification-service-group",
    properties = [
        "group.instance.id=\${instance.id}-orders-created"  // унікальний per listener
    ]
)
fun handleOrderCreated(...) { ... }
```

### max.poll.records та max.poll.interval.ms

```
max.poll.records=10:
  Consumer читає ≤ 10 повідомлень за один poll()
  Менше повідомлень → менший час обробки batch → менший ризик перевищення max.poll.interval

max.poll.interval.ms=300000 (5 хв):
  Якщо між двома poll() > 5 хв → broker: "consumer відпав" → rebalance
  Часта проблема: важка обробка одного batch > max.poll.interval
  Рішення: зменшити max.poll.records або збільшити max.poll.interval.ms
```

### session.timeout.ms та heartbeat.interval.ms

```
heartbeat.interval.ms=15000 (15с):
  Consumer надсилає heartbeat кожні 15с (у background-потоці, незалежно від poll)
  Правило: heartbeat.interval < session.timeout / 3

session.timeout.ms=45000 (45с):
  Broker визнає consumer dead якщо heartbeat не надходив 45с
  Менше = швидше виявлення збою, але ризик false positive при GC pause або high load
  Більше = повільніше виявлення, але стабільніше при тимчасових затримках
```

### Graceful shutdown

```yaml
# notification-service/application.yml
server:
  shutdown: graceful               # Jetty/Tomcat завершує активні запити
  lifecycle:
    timeout-per-shutdown-phase: 30s  # 30с максимум на завершення
```

При SIGTERM:

```
1. Spring: відмовляємо нові HTTP запити
2. Spring Kafka: завершуємо поточний batch
3. Spring Kafka: commitSync() — фіксуємо offset
4. COOPERATIVE: REVOKED призначені партиції → інші consumers отримують їх
5. Graceful exit (без втрати повідомлень)
```

## Як запустити

```bash
docker compose -f docker-compose-08.yml up --build
```

Перевірити 5 контейнерів:

```bash
docker compose -f docker-compose-08.yml ps
# kafka, kafka-ui, order-service-b08
# notification-service-1-b08 (port 8082)
# notification-service-2-b08 (port 8083)
```

## Як протестувати

### 1. Перевірити розподіл партицій

```bash
curl http://localhost:8082/api/notifications/partitions
# {"instanceId":"notification-1","assignedPartitions":["orders.created[0]","orders.created[1]"],"count":2}

curl http://localhost:8083/api/notifications/partitions
# {"instanceId":"notification-2","assignedPartitions":["orders.created[2]"],"count":1}
```

### 2. Переглянути поточну конфігурацію consumer

```bash
curl http://localhost:8082/api/notifications/config
```

Очікувана відповідь:

```json
{
  "instanceId": "notification-1",
  "groupId": "notification-service-group",
  "staticMemberId_created": "notification-1-orders-created",
  "assignmentStrategy": "CooperativeStickyAssignor",
  "maxPollRecords": 10,
  "maxPollIntervalMs": 300000,
  "sessionTimeoutMs": 45000,
  "heartbeatIntervalMs": 15000,
  "ackMode": "MANUAL_IMMEDIATE",
  "gracefulShutdown": true
}
```

### 3. Головний демо-сценарій: COOPERATIVE rebalance

```bash
# Крок 1: Надіслати 6 замовлень
for i in 1 2 3 4 5 6; do
  curl -s -X POST http://localhost:8081/api/orders \
    -H "Content-Type: application/json" \
    -d "{\"userId\":\"user-0$i\",\"product\":\"Book\",\"quantity\":1,\"totalAmount\":25.00}"
done

# Крок 2: Graceful shutdown notification-service-2
docker stop notification-service-2-b08

# Логи notification-service-2 (graceful):
# [GRACEFUL] notification-2: finishing batch, committing offsets...
# [COOPERATIVE] notification-2 REVOKED: [orders.created[2]]

# Логи notification-service-1 (тільки нові партиції, не торкаємось [0],[1]):
# [COOPERATIVE] notification-1 ASSIGNED: [orders.created[2]]

# Крок 3: notification-1 тепер читає всі 3 партиції
curl http://localhost:8082/api/notifications/partitions
# {"instanceId":"notification-1","assignedPartitions":["orders.created[0]","orders.created[1]","orders.created[2]"],"count":3}

# Крок 4: Запустити notification-2 знову
docker start notification-service-2-b08
```

**Windows (PowerShell):**

```powershell
for ($i=1; $i -le 6; $i++) {
  Invoke-RestMethod -Method POST -Uri http://localhost:8081/api/orders `
    -ContentType "application/json" `
    -Body "{`"userId`":`"user-0$i`",`"product`":`"Book`",`"quantity`":1,`"totalAmount`":25.00}"
}
```

### 4. Перевірити static membership через CLI

```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group notification-service-group

# GROUP                       TOPIC              PARTITION  CONSUMER-ID
# notification-service-group  orders.created  0  notification-1-orders-created-...
# notification-service-group  orders.created  1  notification-1-orders-created-...
# notification-service-group  orders.created  2  notification-2-orders-created-...
```

CONSUMER-ID починається з `group.instance.id` → підтверджує static membership.

### 5. Kafka UI

Відкрити: `http://localhost:8080`

- `Consumer Groups` → `notification-service-group` → Members.
- Бачимо `notification-1-orders-created` і `notification-2-orders-created` як member IDs.
- Зупинити notification-2: спостерігати затримку до rebalance (session.timeout = 45с).

## Як це працює всередині

### Producer (без змін порівняно з branch07)

Production-safe конфігурація: `acks=all`, `enable.idempotence=true`, `linger.ms=20`.

### Consumer — notification-service (ключові зміни)

```yaml
# notification-service/application.yml — branch08
server:
  shutdown: graceful                     # ← NEW: graceful shutdown
  lifecycle:
    timeout-per-shutdown-phase: 30s

spring:
  kafka:
    consumer:
      enable-auto-commit: false
      max-poll-records: 10               # ← NEW
      properties:
        partition.assignment.strategy: org.apache.kafka.clients.consumer.CooperativeStickyAssignor  # ← NEW
        max.poll.interval.ms: 300000     # ← NEW
        session.timeout.ms: 45000        # ← NEW
        heartbeat.interval.ms: 15000     # ← NEW
    listener:
      ack-mode: manual_immediate
```

```kotlin
// OrderEventListener.kt — static membership per listener
@KafkaListener(
    topics = ["08.orders.created"],
    groupId = "notification-service-group",
    properties = ["group.instance.id=\${instance.id}-orders-created"]  // ← NEW
)
fun handleOrderCreated(...) { ... }

// ConsumerSeekAware для логування rebalance подій:
override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
    log.info("[COOPERATIVE] {} REVOKED: {}", instanceId, partitions)
    // commitSync() перед виходом
}

override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
    log.info("[COOPERATIVE] {} ASSIGNED: {}", instanceId, partitions)
}
```

## Структура проєкту (зміни відносно branch07)

```
kafka-laboratory/
├── docker-compose-08.yml                     ← 2 notification instances, порти 8082/8083
├── branch08_consumer_configuration/
│   ├── notification-service/
│   │   └── src/main/resources/
│   │       └── application.yml               ← COOPERATIVE, static membership, poll tuning, graceful
│   │   └── src/main/kotlin/.../listener/
│   │       └── OrderEventListener.kt         ← ConsumerSeekAware, group.instance.id, @PreDestroy
│   │   └── src/main/kotlin/.../controller/
│   │       └── NotificationController.kt     ← +/partitions, +/config
│   └── order-service/                        ← топіки 08.orders.*
└── README08.md
```

## Що далі — branch09

- **`@RetryableTopic`** — non-blocking retry через окремі Kafka топіки.
- **Dead Letter Topic (DLT)** — кінцева точка для повідомлень що не вдалося обробити після N спроб.
- **`@DltHandler`** — обробник DLT: логування, alerting, manual review.
- **Exponential backoff** — 1с → 5с перед наступною спробою.
- **Новий сервіс `payment-service`** — замінює notification-service у демо.

---
---

## Слайди для презентації (12 слайдів)

**Слайд 1: Branch 08 — Consumer Configuration**
- Apache Kafka for Certification & Production
- Rebalance strategies, static membership, poll tuning

**Слайд 2: Agenda**
1. EAGER rebalance — stop-the-world проблема
2. COOPERATIVE (Incremental) rebalance
3. CooperativeStickyAssignor
4. Static Group Membership — group.instance.id
5. max.poll.records — захист від timeout
6. session.timeout.ms vs heartbeat.interval.ms
7. max.poll.interval.ms
8. Graceful shutdown
9. Demo: cooperative rebalance в дії
10. Key takeaways & CCDAK

**Слайд 3: EAGER Rebalance — Stop-the-World**
- При будь-якій зміні складу групи: ВСІ consumers зупиняються
- ВСІ партиції REVOKED → усі ASSIGNED заново
- Downtime = час rebalance (секунди до хвилини)
- RangeAssignor, RoundRobinAssignor — EAGER за природою

**Слайд 4: COOPERATIVE Rebalance**
- Тільки "переїжджаючі" партиції тимчасово призупиняються
- Два раунди: спочатку визначаємо що переїде, потім переїздимо
- Інші партиції продовжують читатися без перерви
- CooperativeStickyAssignor — реалізація в Kafka 2.4+

**Слайд 5: Static Group Membership**
- Dynamic: кожен рестарт = 2 rebalance (disconnect + reconnect)
- Static: `group.instance.id` — унікальний per consumer
- Broker чекає session.timeout перш ніж declared dead
- Швидкий рестарт (< session.timeout) → 0 rebalance

**Слайд 6: max.poll.records**
- Обмежує batch за один poll()
- Менше записів → менший час обробки batch
- Захист від перевищення max.poll.interval.ms
- Типове значення production: 10-500 (залежно від обробки)

**Слайд 7: session.timeout.ms vs heartbeat**
- Heartbeat: background-потік, кожні heartbeat.interval.ms
- session.timeout: якщо heartbeat не надходив — consumer dead
- Правило: heartbeat.interval < session.timeout / 3
- Менше session.timeout = швидше виявлення збою, але більше false positives

**Слайд 8: max.poll.interval.ms**
- Різниця від session.timeout: відстежує poll() а не heartbeat
- Якщо обробка batch > max.poll.interval → broker kick-out → rebalance
- Типова помилка: довга DB операція між poll() викликами
- Рішення: зменшити max.poll.records або збільшити max.poll.interval

**Слайд 9: Graceful Shutdown**
- SIGTERM → Spring завершує активні HTTP запити
- Spring Kafka: дочитуємо поточний batch, commitSync()
- COOPERATIVE: REVOKED партиції → інші consumers приймають їх
- Без graceful: незафіксовані offsets + раптовий EAGER rebalance

**Слайд 10: Demo — COOPERATIVE в Дії**
- Стартуємо 2 instances: notification-1 → [0,1], notification-2 → [2]
- `docker stop notification-service-2-b08`
- Лог: notification-2 REVOKED [2], notification-1 ASSIGNED [2]
- notification-1 ніколи не REVOKED [0] і [1] — це COOPERATIVE

**Слайд 11: Key Takeaways**
- **CooperativeStickyAssignor** — production стандарт у Kafka 3.x
- **Static membership** зменшує rebalance при rolling restart
- **max.poll.records** захищає від max.poll.interval timeout
- **heartbeat.interval < session.timeout / 3** — завжди дотримувати
- **Graceful shutdown** = нуль втрачених повідомлень при деплої

**Слайд 12: What's Next — Branch 09: Error Handling & DLT**
- @RetryableTopic — non-blocking retry через Kafka топіки
- Dead Letter Topic (DLT) після N невдалих спроб
- @DltHandler — обробник для manual intervention
- Exponential backoff: 1s → 5s

## Текст для презентації (скрипт)

**Слайд 1:**
Восьма гілка — Consumer Configuration.
Тут ми переходимо від базових концепцій до production-тюнінгу consumer.
Головна тема: як правильно налаштувати rebalance щоб уникнути downtime під час деплою.

**Слайд 2:**
Розглянемо десять тем: від проблеми EAGER rebalance до graceful shutdown.
Ключовий інсайт: rebalance — не просто технічна деталь, це пряма причина downtime у production.

**Слайд 3:**
EAGER rebalance — класичний підхід в ранніх версіях Kafka.
Будь-яка зміна складу групи — новий member або виходить — зупиняє усіх.
Всі партиції звільняються і перерозподіляються заново.
Під час цього весь consumer group стоїть — це stop-the-world.

**Слайд 4:**
COOPERATIVE rebalance вирішує цю проблему.
При rebalance визначаємо лише які партиції потрібно перемістити.
Тільки ці "переїжджаючі" партиції тимчасово призупиняються.
Всі інші продовжують читатись без перерви — мінімальний вплив на throughput.

**Слайд 5:**
Static Group Membership додатково зменшує кількість rebalance.
Без group.instance.id: кожен рестарт сервісу це два rebalance — один коли виходить, другий коли повертається.
З group.instance.id: broker ідентифікує instance і чекає session.timeout перш ніж вважати його мертвим.
Якщо сервіс повернувся за 45 секунд — жодного rebalance.

**Слайд 6:**
max.poll.records обмежує розмір batch за один poll.
Якщо обробка одного batch займає більше max.poll.interval.ms — broker kick-out consumer і починається rebalance.
Зменшивши max.poll.records ми зменшуємо час обробки batch і уникаємо цієї проблеми.
Типова помилка в production: мала кількість партицій але великий batch з повільною обробкою.

**Слайд 7:**
session.timeout і heartbeat — два механізми виявлення збоїв.
Heartbeat відправляється в background-потоці незалежно від poll.
Якщо heartbeat не надходить session.timeout.ms — broker вважає consumer мертвим.
Правило трьох: heartbeat.interval повинен бути менше session.timeout поділеного на три.

**Слайд 8:**
max.poll.interval відстежує не heartbeat а poll.
Якщо між двома poll занадто великий інтервал — consumer вважається застряглим.
Це може статись при довгій синхронній операції між poll викликами — наприклад запит до БД.
Рішення: зменшити max.poll.records або перенести важку роботу в async.

**Слайд 9:**
Graceful shutdown критично важливий для нуля втрат при деплої.
При SIGTERM Spring завершує поточний HTTP запит, Kafka дочитує поточний batch.
commitSync фіксує offset — наступний consumer instance починає з правильного місця.
COOPERATIVE rebalance завершує процес чисто без зайвих затримок.

**Слайд 10:**
У демо запускаємо два instances і зупиняємо один.
Спостерігаємо логи: notification-2 REVOKED тільки свою партицію.
notification-1 не переривається, просто отримує одну додаткову партицію.
Це і є сила COOPERATIVE rebalance — мінімальний вплив на роботу системи.

**Слайд 11:**
Підсумок: CooperativeStickyAssignor є production-стандартом починаючи з Kafka 3.0.
Static membership додатково зменшує rebalance при rolling restart.
max.poll.records і правило session/heartbeat — базовий тюнінг для будь-якого consumer.
Graceful shutdown — це нуль втрачених повідомлень при деплої.

**Слайд 12:**
Наступна гілка — Error Handling і Dead Letter Topic.
Розберемо що робити коли consumer не може обробити повідомлення.
@RetryableTopic автоматично перекидає невдалі повідомлення в retry-топіки.
Після N спроб — Dead Letter Topic з @DltHandler для ручного розгляду.

## Тестові питання (до 10 питань)

**Питання 1:**
Який rebalance відбудеться при зупинці одного з трьох consumers у групі
з `CooperativeStickyAssignor`?

A) Усі три consumers REVOKED усі партиції → перерозподіл заново (EAGER)
B) Тільки партиції зупиненого consumer REVOKED → перерозподіл на решту
C) Жодного rebalance — CooperativeStickyAssignor не реагує на зміни
D) Один raund REVOKED усі → другий раунд ASSIGNED частково

**Відповідь:** B —
COOPERATIVE: тільки партиції що потрібно перемістити тимчасово REVOKED.
Інші consumers продовжують читати свої партиції без перерви.

---

**Питання 2:**
Consumer з `session.timeout.ms=45000` і `heartbeat.interval.ms=15000` завис через GC pause на 20 секунд.
Що відбудеться?

A) Broker визнає consumer dead → rebalance
B) Consumer повернеться до роботи нормально — 20с < 45с
C) Kafka автоматично збільшить session.timeout
D) Consumer отримає CommitFailedException

**Відповідь:** B —
Heartbeat відправляється в background-потоці незалежно від GC.
20с < session.timeout (45с) → broker отримав heartbeat у межах ліміту → OK.

---

**Питання 3:**
`max.poll.records=500`. Обробка одного batch займає 10 хвилин.
`max.poll.interval.ms=300000` (5 хв). Що відбудеться?

A) Consumer обробить batch за 10 хвилин, потім poll наступний
B) Broker виключить consumer з групи → rebalance після 5 хвилин
C) Spring Kafka автоматично знизить max.poll.records до безпечного значення
D) Consumer отримає TimeoutException і перезапуститься

**Відповідь:** B —
max.poll.interval.ms = 5 хв, обробка = 10 хв > 5 хв.
Broker не отримав poll за 5 хвилин → consumer виключається → rebalance.
Рішення: зменшити max.poll.records або збільшити max.poll.interval.ms.

---

**Питання 4:**
Яка переваг `group.instance.id` (static membership) порівняно з dynamic membership?

A) Consumer обробляє повідомлення швидше
B) Broker ідентифікує instance і чекає session.timeout перш ніж kick-out → менше rebalance
C) Consumer не потребує heartbeat
D) Kafka не записує offset для static members

**Відповідь:** B —
Static membership: broker знає що instance X — це той самий consumer що й раніше.
При рестарті broker чекає session.timeout замість негайного kick-out.
Якщо instance повернувся до timeout — rebalance не відбувається.

---

**Питання 5:**
Яке правило для `heartbeat.interval.ms` відносно `session.timeout.ms`?

A) heartbeat.interval = session.timeout
B) heartbeat.interval < session.timeout / 3
C) heartbeat.interval = session.timeout * 2
D) heartbeat.interval > session.timeout

**Відповідь:** B —
Правило: `heartbeat.interval.ms < session.timeout.ms / 3`.
Це гарантує щонайменше 3 heartbeat до session.timeout.
Наприклад: session=45s → heartbeat < 15s.

---

**Питання 6:**
В чому різниця між `session.timeout.ms` і `max.poll.interval.ms`?

A) session.timeout відстежує heartbeat; max.poll.interval відстежує частоту poll()
B) session.timeout відстежує poll(); max.poll.interval відстежує heartbeat
C) Це одне і те саме під різними іменами
D) session.timeout для producer; max.poll.interval для consumer

**Відповідь:** A —
session.timeout: broker не отримав heartbeat за N ms → consumer dead.
max.poll.interval: consumer не викликав poll() за N ms → consumer dead.
Обидва незалежно можуть тригернути rebalance.

---

**Питання 7:**
Consumer з `CooperativeStickyAssignor` отримує нові партиції при rebalance.
Старі партиції цього consumer `REVOKED`?

A) Так — всі партиції завжди REVOKED при rebalance
B) Ні — тільки якщо вони переходять до іншого consumer
C) Так — але тільки в першому раунді
D) Не завжди — залежить від `sticky` налаштування

**Відповідь:** B —
CooperativeStickyAssignor: partition залишається у того самого consumer якщо це можливо.
REVOKED відбувається тільки для партицій що переміщуються до іншого consumer.
Нові партиції просто ASSIGNED без попереднього REVOKED.

---

**Питання 8:**
Яка переваг `server.shutdown=graceful` для Kafka consumer?

A) Consumer повідомлення не обробляються під час shutdown
B) Consumer завершує поточний batch і commitSync перед виходом → нуль втрат
C) Kafka UI показує graceful containers інакше
D) Consumer отримує більше часу між heartbeat

**Відповідь:** B —
Graceful shutdown дає Spring Kafka час завершити поточний batch і зафіксувати offset.
Наступний consumer instance починає з правильного offset → нуль дублікатів при rolling restart.

---

**Питання 9:**
Два consumers у групі: notification-1 → [0,1], notification-2 → [2].
notification-1 виходить з групи (COOPERATIVE).
Що відбувається?

A) Усі три consumers REVOKED → перерозподіл
B) notification-1 REVOKED [0,1] → notification-2 ASSIGNED [0,1]; [2] залишається
C) notification-2 залишається з [2]; нового assignment не буде
D) Partition [0] і [1] залишаються без consumer до наступного rebalance

**Відповідь:** B —
notification-1 виходить: партиції [0,1] стають "вільними".
COOPERATIVE: тільки [0,1] REVOKED і ASSIGNED до notification-2.
[2] у notification-2 не торкається.

---

**Питання 10:**
Яке налаштування НЕ впливає на частоту rebalance в consumer group?

A) `session.timeout.ms`
B) `group.instance.id`
C) `max.poll.records`
D) `partition.assignment.strategy`

**Відповідь:** C —
`max.poll.records` впливає на кількість повідомлень за poll, але НЕ на частоту rebalance напряму.
(Непряма залежність: менший batch → швидша обробка → менший ризик max.poll.interval timeout.)
A, B, D напряму впливають на rebalance поведінку.