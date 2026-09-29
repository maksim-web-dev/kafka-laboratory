# Branch 05 — Multiple Consumer Groups

## `branch05_multiple_consumer_groups` — що вивчаємо

- **Дві незалежні consumer groups читають один топік одночасно** — жодна не заважає іншій.
- **Незалежні offset-и** — кожна group зберігає свій прогрес читання в `__consumer_offsets`.
- **Kafka не видаляє повідомлення після читання** — на відміну від черг типу RabbitMQ.
- **Fan-out pattern** — один producer, необмежена кількість consumer groups, без копій даних.
- **Consumer Lag** — різниця між поточним offset'ом групи та кінцем партиції (LOG-END-OFFSET).
- **auto-offset-reset: earliest** — нова group читає топік з самого початку.
- **Replay** — скидання offset через `--reset-offsets --to-earliest` для перечитування усіх подій.
- **Новий сервіс `analytics-service`** — слухає `05.orders.created` і накопичує статистику в пам'яті.
- **`ConcurrentHashMap` + `AtomicInteger`** — thread-safe акумулятори для паралельних consumer-потоків.
- **`kafka-consumer-groups --describe`** — CLI-інструмент для моніторингу lag кожної групи.

## Що змінилося порівняно з branch04

- Замість 3 екземплярів `notification-service` — тепер **1 екземпляр** (спрощення для демо).
- Доданий новий сервіс **`analytics-service`** з власною consumer group `analytics-service-group`.
- `analytics-service` підписується лише на `05.orders.created` — не слухає `05.orders.cancelled`.
- Нові REST-ендпоінти: `GET /api/analytics/stats` та `GET /api/analytics/stats/user/{userId}`.
- `analytics-service` використовує `auto-offset-reset: earliest` — читає з початку топіку при першому запуску.
- Порт `analytics-service`: **8084** (відповідно до стандартизованої схеми).
- Назви топіків змінені на `05.orders.*` (відповідно до нумерації гілки).

## Архітектура

```
┌─────────────────────────────────────────────────────────────────┐
│  order-service :8081                                            │
│  POST /api/orders  →  KafkaTemplate.send(key=userId)            │
└───────────────────────┬─────────────────────────────────────────┘
                        │  key=userId
                        ▼
        ╔═══════════════════════════════╗
        ║   05.orders.created           ║
        ║   partitions: 3               ║
        ║   retention: 7 days           ║
        ╠══════════╦══════════╦═════════╣
        ║ Part. 0  ║ Part. 1  ║ Part. 2 ║
        ╚═════╤════╩═════╤════╩════╤════╝
              │          │         │
    ┌─────────┴──────────┴─────────┴──────────┐
    │                                          │
    ▼                                          ▼
notification-service-group          analytics-service-group
notification-service-b05 :8082      analytics-service-b05 :8084
→ надсилає email                    → рахує статистику
offset: незалежний ✓                offset: незалежний ✓
```

```
__consumer_offsets (внутрішній топік Kafka):

notification-service-group | 05.orders.created | partition 0 → offset 15
notification-service-group | 05.orders.created | partition 1 → offset 12
notification-service-group | 05.orders.created | partition 2 → offset 18

analytics-service-group    | 05.orders.created | partition 0 → offset 8   ← відстає!
analytics-service-group    | 05.orders.created | partition 1 → offset 12
analytics-service-group    | 05.orders.created | partition 2 → offset 18
```

### Топіки та їх налаштування

- `05.orders.created` → партиції: 3, retention: 7 днів (604 800 000 мс),
  читається двома групами: `notification-service-group` та `analytics-service-group`.
- `05.orders.cancelled` → партиції: 1, retention: 7 днів,
  читається лише `notification-service-group`.

## Ключові концепції цієї гілки

### Multiple Consumer Groups — незалежне читання одного топіку

Будь-яка кількість consumer groups може читати один і той самий топік незалежно одна від одної.
Kafka зберігає повідомлення незалежно від того, хто і скільки разів їх прочитав.
Кожна group "бачить" топік так, ніби вона єдиний читач.

```
Producer → 05.orders.created:
  offset 0: OrderCreated{userId="user-42"}
  offset 1: OrderCreated{userId="user-99"}
  offset 2: OrderCreated{userId="user-42"}

notification-service-group (partition 1):
  читає offset 0 ✓ → email sent
  читає offset 1 ✓ → email sent
  committed offset = 2

analytics-service-group (той самий partition 1):
  читає offset 0 ✓ → counted
  читає offset 1 ✓ → counted
  committed offset = 2
  (незалежно від notification)
```

> Читання однією group **не просуває** offset іншої group — кожна зберігає свій прогрес.

### Незалежні offset-и в `__consumer_offsets`

Kafka зберігає committed offset **окремо для кожної пари (group, topic, partition)**.
Тобто для 2 груп і 3 партицій — 6 незалежних записів в `__consumer_offsets`.

```
ключ запису: (group-id, topic-name, partition)
значення:    committed-offset

("notification-service-group", "05.orders.created", 0) → 15
("notification-service-group", "05.orders.created", 1) → 12
("analytics-service-group",    "05.orders.created", 0) → 8
("analytics-service-group",    "05.orders.created", 1) → 12
```

### Consumer Lag — відставання групи

**Lag** показує скільки повідомлень consumer group ще не прочитала.

```
LAG = LOG-END-OFFSET − COMMITTED-OFFSET

Приклад:
  partition 0: LOG-END = 10, committed = 8  →  LAG = 2
  partition 1: LOG-END = 12, committed = 12 →  LAG = 0
```

- LAG = 0 — group в реальному часі, повідомлення обробляються одразу.
- LAG > 0 — group відстає: сервіс зупинений, або обробляє повільніше ніж надходить.
- Моніторинг lag — ключовий індикатор здоров'я consumer у production.

### Fan-out pattern — один producer, N consumer groups

```
order-service
    │  publish once
    ▼
05.orders.created
    ├── notification-service-group  → email-сповіщення
    ├── analytics-service-group     → статистика
    └── (future) audit-service-group → аудит
```

Producer публікує повідомлення **один раз**.
Kafka зберігає дані і дозволяє будь-якій кількості груп прочитати їх незалежно.
Немає дублювання даних, немає координації між консьюмерами.

### auto-offset-reset: earliest vs latest

Налаштування `auto-offset-reset` визначає з якого місця group починає читати
якщо для неї ще **немає committed offset** (перший запуск або нова group).

```yaml
# analytics-service/application.yml
spring:
  kafka:
    consumer:
      auto-offset-reset: earliest  # читати з початку топіку
```

- `earliest` — читати всі повідомлення з початку (від offset 0 кожної партиції).
  Потрібно для analytics/audit, де важлива повна картина.
- `latest` — читати лише нові повідомлення, що надійдуть **після** запуску.
  Підходить для real-time сервісів, яким старі дані не потрібні.

> `notification-service` теж використовує `earliest` — щоб не пропустити
> жодного замовлення навіть якщо сервіс стартує пізніше за producer.

### Replay — перечитування усіх подій

`--reset-offsets --to-earliest` скидає committed offset групи до початку топіку.
При наступному запуску group перечитає **всі** збережені повідомлення.

```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group analytics-service-group \
  --topic 05.orders.created \
  --reset-offsets --to-earliest --execute
```

Це корисно коли:
- запустили новий сервіс і треба обробити старі події.
- виникла помилка в логіці і треба переобробити дані.
- тестування: потрібно очистити стан і почати з нуля.

### ConcurrentHashMap + AtomicInteger — thread-safe статистика

Spring Kafka за замовчуванням запускає **кілька потоків** для паралельної обробки партицій.
Тому `OrderAnalyticsListener` використовує thread-safe структури:

```kotlin
private val totalOrders = AtomicInteger(0)
private val totalRevenueCents = AtomicLong(0)
private val ordersByUser = ConcurrentHashMap<String, AtomicInteger>()

@KafkaListener(topics = ["05.orders.created"], groupId = "analytics-service-group")
fun handleOrderCreated(record: ConsumerRecord<String, OrderCreatedEvent>) {
    val event = record.value()
    val total = totalOrders.incrementAndGet()
    val amountCents = (event.totalAmount * 100).toLong()
    totalRevenueCents.addAndGet(amountCents)
    ordersByUser.computeIfAbsent(event.userId) { AtomicInteger(0) }.incrementAndGet()

    log.info("[ANALYTICS] Order counted → userId={}, partition={}, offset={}, totalOrders={}",
        event.userId, record.partition(), record.offset(), total)
}
```

`AtomicInteger.incrementAndGet()` і `AtomicLong.addAndGet()` — атомарні операції,
безпечні при одночасному доступі з кількох потоків без явних блокувань.

## Як запустити

```bash
docker compose -f docker-compose-05.yml up --build
```

Перевірити 5 контейнерів:

```bash
docker compose -f docker-compose-05.yml ps
# kafka, kafka-ui, order-service-b05
# notification-service-b05, analytics-service-b05
```

## Як протестувати

### 1. Надіслати кілька замовлень (Linux)

```bash
for user in user-01 user-02 user-01 user-03 user-02 user-01; do
  curl -s -X POST http://localhost:8081/api/orders \
    -H "Content-Type: application/json" \
    -d "{\"userId\":\"$user\",\"product\":\"Book\",\"quantity\":1,\"totalAmount\":25.00}"
done
```

**Windows (PowerShell):**

```powershell
foreach ($user in @("user-01","user-02","user-01","user-03","user-02","user-01")) {
  Invoke-RestMethod -Method POST -Uri "http://localhost:8081/api/orders" `
    -ContentType "application/json" `
    -Body "{`"userId`":`"$user`",`"product`":`"Book`",`"quantity`":1,`"totalAmount`":25.00}"
}
```

### 2. Перевірити що обидві групи отримали всі події

```bash
# notification-service — лічильник email-сповіщень
curl http://localhost:8082/api/notifications/count
# {"instanceId":"notification-service-1","orders_created":6,"orders_cancelled":0,"total":6}

# analytics-service — загальна статистика
curl http://localhost:8084/api/analytics/stats
```

Очікувана відповідь analytics:

```json
{
  "consumerGroup": "analytics-service-group",
  "totalOrders": 6,
  "totalRevenue": "150.00",
  "uniqueUsers": 3,
  "ordersByUser": { "user-01": 3, "user-02": 2, "user-03": 1 },
  "revenueByUser": { "user-01": "75.00", "user-02": "50.00", "user-03": "25.00" }
}
```

### 3. Статистика per user

```bash
curl http://localhost:8084/api/analytics/stats/user/user-01
# {"userId":"user-01","orders":3,"revenue":"75.00"}
```

**Windows (PowerShell):**

```powershell
Invoke-RestMethod http://localhost:8084/api/analytics/stats/user/user-01
```

### 4. Головний демо-сценарій: зупинити analytics, надіслати повідомлення, запустити

```bash
# Крок 1: Зупинити analytics-service
docker stop analytics-service-b05
echo "Analytics зупинено"

# Крок 2: Надіслати 5 нових замовлень (notification отримає, analytics — ні)
for i in 1 2 3 4 5; do
  curl -s -X POST http://localhost:8081/api/orders \
    -H "Content-Type: application/json" \
    -d "{\"userId\":\"user-missed\",\"product\":\"Missed Order #$i\",\"quantity\":1,\"totalAmount\":10.00}"
done

# Крок 3: Notification отримала всі повідомлення
curl http://localhost:8082/api/notifications/count
# total: 11

# Крок 4: Запустити analytics знову
docker start analytics-service-b05

# Крок 5: Через 5-10 секунд analytics дочитав пропущені
curl http://localhost:8084/api/analytics/stats
# totalOrders: 11  ← дочитав всі
# ordersByUser.user-missed: 5  ← обробив пропущені
```

### 5. Перевірити lag через CLI

```bash
# Поки analytics зупинений — lag > 0:
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group analytics-service-group

# GROUP                    TOPIC              PARTITION  OFFSET  LOG-END  LAG
# analytics-service-group  05.orders.created  0          2       4        2
# analytics-service-group  05.orders.created  1          3       5        2
# analytics-service-group  05.orders.created  2          1       3        2

# notification-service не відстає:
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group notification-service-group
# LAG = 0 для всіх партицій
```

### 6. Kafka UI — дві consumer groups

Відкрити: `http://localhost:8080`

- `Consumer Groups` → два записи:
  - `notification-service-group` (lag = 0, 1 member)
  - `analytics-service-group` (lag = 0 або > 0 якщо зупинений)
- Порівняти offsets кожної групи — вони незалежні.
- `Topics` → `05.orders.created` → `Consumers` → обидві групи видно на одній вкладці.

### 7. Replay: скинути offset analytics до початку

```bash
# Зупинити analytics (group не може читати поки відкрита)
docker stop analytics-service-b05

# Скинути offset до найстарішого повідомлення
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group analytics-service-group \
  --topic 05.orders.created \
  --reset-offsets --to-earliest --execute

# Запустити analytics знову — він перечитає ВСІ повідомлення з початку
docker start analytics-service-b05
```

## Як це працює всередині

### Producer (зміни відносно branch04)

Назви топіків змінились з `04.orders.*` на `05.orders.*` — лише рефакторинг:

```kotlin
// OrderService.kt
private val topicCreated = "05.orders.created"
private val topicCancelled = "05.orders.cancelled"

fun createOrder(...): OrderCreatedEvent {
    kafkaTemplate.send(topicCreated, userId, event)  // ключ = userId (без змін)
}
```

### Consumer — notification-service (без змін порівняно з branch04)

Слухає `05.orders.created` з groupId `notification-service-group`.
Логіка і структура — ідентичні branch04, лише одна інстанція:

```kotlin
@KafkaListener(topics = ["05.orders.created"], groupId = "notification-service-group")
fun handleOrderCreated(record: ConsumerRecord<String, OrderCreatedEvent>) {
    log.info("║  Instance  : {}", instanceId)
    log.info("║  Key: {}  →  Partition: {}  Offset: {}", record.key(), record.partition(), record.offset())
    log.info("║  → Email sent to user {}", event.userId)
}
```

### Consumer — analytics-service (НОВИЙ)

Інша group ID, `earliest` offset reset, thread-safe акумулятори:

```kotlin
// OrderAnalyticsListener.kt
@KafkaListener(topics = ["05.orders.created"], groupId = "analytics-service-group")
fun handleOrderCreated(record: ConsumerRecord<String, OrderCreatedEvent>) {
    val event = record.value()
    totalOrders.incrementAndGet()
    totalRevenueCents.addAndGet((event.totalAmount * 100).toLong())
    ordersByUser.computeIfAbsent(event.userId) { AtomicInteger(0) }.incrementAndGet()

    log.info("[ANALYTICS] Order counted → userId={}, partition={}, offset={}, totalOrders={}",
        event.userId, record.partition(), record.offset(), totalOrders.get())
}
```

```yaml
# analytics-service/application.yml
server:
  port: 8084

spring:
  kafka:
    consumer:
      group-id: analytics-service-group   # ← інша group, ніж у notification
      auto-offset-reset: earliest          # ← читати з початку при першому старті
```

## Структура проєкту (зміни відносно branch04)

```
kafka-laboratory/
├── docker-compose-05.yml               ← 1 notification (не 3), + analytics-service
├── branch05_multiple_consumer_groups/
│   ├── analytics-service/              ← NEW сервіс
│   │   ├── Dockerfile
│   │   ├── build.gradle.kts
│   │   └── src/main/kotlin/com/kafkalab/analytics/
│   │       ├── AnalyticsServiceApplication.kt
│   │       ├── controller/
│   │       │   └── AnalyticsController.kt    ← /stats, /stats/user/{id}
│   │       ├── listener/
│   │       │   └── OrderAnalyticsListener.kt ← analytics-service-group
│   │       └── model/
│   │           └── OrderCreatedEvent.kt
│   │   └── src/main/resources/
│   │       └── application.yml               ← group-id: analytics-service-group
│   ├── notification-service/           ← 1 інстанс (спрощено від branch04)
│   │   └── src/main/resources/
│   │       └── application.yml               ← group-id: notification-service-group
│   └── order-service/                  ← топіки 05.orders.*
│       └── src/main/kotlin/.../config/
│           └── KafkaTopicConfig.kt           ← 05.orders.created (3 part.)
└── README05.md
```

## Що далі — branch06

- **Auto commit** (`enable.auto.commit=true`) — ризики та налаштування інтервалу.
- **Manual commit** — `commitSync()` vs `commitAsync()` та коли що застосовувати.
- **Offset reset strategies** — `earliest`, `latest`, `none` і коли кожна доречна.
- **At-least-once vs at-most-once** — гарантії доставки залежно від стратегії commit.
- **Симуляція збою під час обробки** — що відбувається з uncommitted offset'ом.

---
---

## Слайди для презентації (12 слайдів)

**Слайд 1: Branch 05 — Multiple Consumer Groups**
- Apache Kafka for Certification & Production
- Два незалежних consumer читають один топік

**Слайд 2: Agenda**
1. Проблема: як два сервіси отримають одні й ті ж дані?
2. Multiple Consumer Groups — принцип роботи
3. Незалежні offset-и в `__consumer_offsets`
4. Fan-out pattern — один producer, N груп
5. Consumer Lag — моніторинг відставання
6. auto-offset-reset: earliest vs latest
7. Replay — скидання offset до початку
8. Demo: зупинити analytics → надіслати → запустити
9. Thread-safe акумулятори: AtomicInteger + ConcurrentHashMap
10. Key takeaways & CCDAK

**Слайд 3: Як два сервіси отримають одні й ті ж дані?**
- Kafka не видаляє повідомлення після читання
- Повідомлення зберігаються до закінчення `retention.ms` (за замовчуванням 7 днів)
- Будь-яка кількість consumer groups читає топік незалежно
- Кожна group "бачить" топік з початку свого offset'у

**Слайд 4: Multiple Consumer Groups — архітектура**
- ASCII-діаграма: order-service → 05.orders.created → notification-group + analytics-group
- Два незалежні потоки читання
- notification: надсилає email
- analytics: рахує статистику

**Слайд 5: Незалежні offset-и**
- `__consumer_offsets` — внутрішній топік Kafka
- Ключ: (group-id, topic-name, partition)
- notification offset і analytics offset — абсолютно незалежні
- Читання одного не впливає на прогрес іншого

**Слайд 6: Consumer Lag**
- LAG = LOG-END-OFFSET − COMMITTED-OFFSET
- LAG = 0: group в реальному часі
- LAG > 0: group відстає (зупинена або повільна)
- Ключовий production-метрик: alert якщо LAG > порогу

**Слайд 7: auto-offset-reset: earliest vs latest**
- `earliest` — читати всі повідомлення з початку (analytics, audit)
- `latest` — читати лише нові повідомлення (real-time UI, notifications)
- Застосовується тільки при ПЕРШОМУ запуску групи (або після replay)

**Слайд 8: Fan-out Pattern**
- Один producer публікує один раз
- Кількість груп не обмежена
- Немає дублювання даних у топіку
- Кожна група незалежно вирішує що робити з даними

**Слайд 9: Demo — Зупинити → Надіслати → Запустити**
- Зупинити analytics-service-b05
- Надіслати 5 замовлень (notification отримає, analytics — ні)
- Запустити analytics знову
- Analytics дочитує пропущені завдяки збереженому offset'у

**Слайд 10: Replay**
- `--reset-offsets --to-earliest` скидає offset до початку
- Group перечитає всі збережені повідомлення
- Застосування: новий сервіс, помилка в логіці, тестування
- Kafka зберігає дані — replay завжди можливий у межах retention

**Слайд 11: Thread-safe акумулятори**
- `AtomicInteger` / `AtomicLong` — атомарні операції без блокувань
- `ConcurrentHashMap` — паралельний доступ з кількох потоків
- Spring Kafka може обробляти кілька партицій паралельно
- Уникаємо race condition при підрахунку статистики

**Слайд 12: Key Takeaways**
- Незалежні offset-и гарантують ізоляцію між групами
- Consumer Lag — головний метрик для production-моніторингу
- Fan-out без копіювання даних — ключова перевага Kafka над чергами
- Replay можливий у межах retention.ms
- auto-offset-reset: earliest vs latest — вибір залежить від бізнес-вимог

## Текст для презентації (скрипт)

**Слайд 1:**
Вітаємо у п'ятій гілці нашого курсу.
До цього ми розбирали як одна consumer group читає топік.
Сьогодні вчимось давати доступ до одних і тих самих даних кільком незалежним сервісам одночасно.

**Слайд 2:**
Розглянемо десять тем: від основного принципу multiple groups до thread-safe реалізації.
Головний інсайт: Kafka — не черга з одним читачем, а журнал подій для необмеженої кількості споживачів.

**Слайд 3:**
Перше питання: чому два сервіси можуть отримати одні й ті самі повідомлення?
Kafka не видаляє повідомлення після читання — це ключова відмінність від RabbitMQ або ActiveMQ.
Дані зберігаються до закінчення retention.ms — за замовчуванням 7 днів.
Тому будь-яка кількість consumer groups може читати топік зі своєї позиції.

**Слайд 4:**
В нашій архітектурі order-service публікує замовлення в `05.orders.created`.
notification-service-group читає ці повідомлення і надсилає email.
analytics-service-group читає ті самі повідомлення і рахує статистику.
Жоден з них не знає про існування іншого — повна ізоляція.

**Слайд 5:**
Kafka зберігає committed offset для кожної комбінації (group, topic, partition) у внутрішньому топіку `__consumer_offsets`.
notification offset 15 і analytics offset 8 для однієї партиції — це два незалежних числа.
Analytics може відставати, але це жодним чином не впливає на notification.

**Слайд 6:**
Consumer Lag — це різниця між кінцем партиції і тим, де зараз знаходиться группа.
LAG нуль означає що група обробляє повідомлення швидше ніж вони надходять.
LAG більше нуля — група відстає: або повільна обробка, або сервіс зупинений.
У production: alert якщо lag перевищує пороговий рівень.

**Слайд 7:**
`auto-offset-reset` — важливе налаштування для нових груп.
`earliest`: читати з самого початку топіку — підходить для analytics, audit, data-warehouse.
`latest`: читати лише нові повідомлення — підходить для real-time сервісів, де старі дані нерелевантні.
Analytics використовує `earliest` — щоб не пропустити жодного замовлення.

**Слайд 8:**
Fan-out — паттерн де один source генерує дані для багатьох споживачів.
У Kafka producer публікує один раз, а дані зберігаються.
Будь-яка нова consumer group підключається і читає все що їй потрібно.
Немає потреби в додаткових копіях або routing-логіці.

**Слайд 9:**
Демо у нас три кроки.
Зупиняємо analytics-service — він більше не читає топік.
Надсилаємо 5 замовлень — notification отримує і обробляє їх.
Запускаємо analytics знову — він дочитує всі пропущені повідомлення з offset-у де зупинився.

**Слайд 10:**
Replay — відновлення стану від початку топіку.
Скидаємо committed offset через CLI-команду `--reset-offsets --to-earliest`.
При наступному запуску group перечитає всі збережені повідомлення.
Корисно при помилці в бізнес-логіці або запуску нового analytics-сервісу на старих даних.

**Слайд 11:**
Spring Kafka може паралельно обробляти кілька партицій — кожна в своєму потоці.
Якщо кілька потоків одночасно оновлюють лічильники, звичайний int або HashMap дають race condition.
Використовуємо `AtomicInteger.incrementAndGet()` і `ConcurrentHashMap.computeIfAbsent()` — операції атомарні без явних lock-ів.

**Слайд 12:**
Ключовий висновок: Kafka — це лог подій, а не черга.
Multiple consumer groups — головна відмінність від традиційних брокерів.
Consumer lag — перший метрик, який моніторять у production.
Replay робить Kafka унікальним інструментом для event-driven архітектур.

## Тестові питання (до 10 питань)

**Питання 1:**
Два consumer groups (`notification-group` і `analytics-group`) читають один і той самий топік.
`notification-group` знаходиться на offset 100.
Що відбудеться з offset'ом `analytics-group`?

A) Також переміститься на offset 100
B) Залишиться незмінним — у кожної group власний offset
C) Kafka видалить повідомлення до offset 100
D) `analytics-group` отримає помилку конфлікту

**Відповідь:** B —
Кожна consumer group зберігає власний committed offset незалежно.
Читання однієї групи не впливає на прогрес іншої.

---

**Питання 2:**
Що зберігає Kafka у внутрішньому топіку `__consumer_offsets`?

A) Самі повідомлення з усіх топіків
B) Committed offset для кожної комбінації (group, topic, partition)
C) Список активних consumer-екземплярів
D) Конфігурацію broker-ів

**Відповідь:** B —
`__consumer_offsets` зберігає пари ключ-значення де ключ це `(group-id, topic-name, partition)`,
а значення — останній committed offset цієї групи в цій партиції.

---

**Питання 3:**
Consumer Lag партиції = 15.
LOG-END-OFFSET цієї партиції = 20.
Який committed offset у consumer group?

A) 5
B) 15
C) 20
D) 35

**Відповідь:** A —
LAG = LOG-END-OFFSET − COMMITTED-OFFSET → 15 = 20 − COMMITTED → COMMITTED = 5.

---

**Питання 4:**
`analytics-service` запускається вперше (для нього ще немає committed offset).
В конфігурації: `auto-offset-reset: earliest`.
Яку поведінку він матиме?

A) Прочитає лише нові повідомлення після запуску
B) Прочитає всі збережені повідомлення з початку топіку
C) Згенерує помилку через відсутність offset'у
D) Почне з середини топіку

**Відповідь:** B —
`auto-offset-reset: earliest` вказує починати з найстарішого доступного offset'у
(offset 0 або перший, що ще не видалений через retention).

---

**Питання 5:**
Яке налаштування `auto-offset-reset` підходить для real-time notification-сервісу,
якому старі події не потрібні?

A) `earliest`
B) `latest`
C) `none`
D) `reset`

**Відповідь:** B —
`latest` означає починати з кінця топіку — читати лише нові повідомлення після запуску.
`earliest` підходить для analytics і audit.
`none` генерує виключення якщо немає committed offset'у — використовується в спеціальних сценаріях.

---

**Питання 6:**
Яка команда скидає committed offset групи до початку топіку?

A) `kafka-consumer-groups --reset-offsets --to-latest`
B) `kafka-consumer-groups --reset-offsets --to-earliest --execute`
C) `kafka-topics --delete-offsets`
D) `kafka-consumer-groups --clear-offsets`

**Відповідь:** B —
`--reset-offsets --to-earliest --execute` скидає committed offset до найстарішого повідомлення.
Без `--execute` команда лише показує що буде змінено (dry-run).

---

**Питання 7:**
Чому analytics-service використовує `ConcurrentHashMap` замість звичайного `HashMap`?

A) `ConcurrentHashMap` швидший для одиночного потоку
B) Spring Kafka може обробляти кілька партицій в паралельних потоках
C) Kafka вимагає thread-safe структур за специфікацією
D) `HashMap` не підтримує String ключі

**Відповідь:** B —
Spring Kafka може призначити кілька партицій одному consumer-екземпляру і обробляти їх паралельно.
Без thread-safe структур можливий race condition при паралельному оновленні лічильників.

---

**Питання 8:**
В чому головна відмінність Kafka від черги (RabbitMQ, ActiveMQ) з точки зору множинних споживачів?

A) Kafka підтримує більше одного consumer
B) Kafka не видаляє повідомлення після читання — вони доступні всім групам
C) Kafka автоматично розсилає копії кожному consumer'у
D) У Kafka немає consumer groups

**Відповідь:** B —
У традиційних чергах повідомлення видаляється після доставки одному споживачу.
Kafka зберігає повідомлення до закінчення retention — тому будь-яка кількість груп читає незалежно.

---

**Питання 9:**
Компанія запускає новий audit-service і хоче, щоб він прочитав усі замовлення за останні 7 днів.
Топік `orders.created` має retention.ms = 604800000 (7 днів).
Які кроки потрібні?

A) Запустити audit-service з `auto-offset-reset: earliest` — він прочитає все сам
B) Скопіювати повідомлення вручну з іншого сервісу
C) Зупинити Kafka і перезапустити з новими налаштуваннями
D) Підписати audit-service на існуючу notification-service-group

**Відповідь:** A —
Нова consumer group не має committed offset.
`auto-offset-reset: earliest` змусить її розпочати з offset 0.
Kafka зберігає всі повідомлення в межах retention, тому audit отримає всі 7 днів.

---

**Питання 10:**
analytics-service зупинений 30 хвилин.
За цей час notification-service отримала 200 нових замовлень (LAG analytics = 200).
Що відбудеться коли analytics-service стартує знову?

A) Він пропустить 200 повідомлень — вони вже "старі"
B) Він дочитає всі 200 пропущених повідомлень із збереженого offset'у
C) Kafka видалить пропущені повідомлення щоб синхронізувати offset-и
D) analytics-service згенерує помилку через великий LAG

**Відповідь:** B —
Kafka зберігає повідомлення незалежно від того чи прочитала їх якась group.
При рестарті analytics-service продовжить читання з останнього committed offset'у
і дочитає всі 200 пропущених повідомлень.