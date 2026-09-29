# Branch 04 — Consumer Groups

---

## `branch04_customer_groups` — що вивчаємо

- Consumer Group — як Kafka розподіляє партиції між екземплярами одного сервісу
- Правило: одна партиція читається **тільки одним** consumer-ом у групі одночасно
- Запуск 3 екземплярів `notification-service` в одній групі — кожен отримує 1 партицію
- Rebalance — автоматичний перерозподіл партицій при підключенні або відключенні consumer-а
- `RangeAssignor` vs `RoundRobinAssignor` — стратегії призначення партицій
- `ConsumerSeekAware` — відстеження partition assignment у Spring Kafka
- Static Group Membership (`group.instance.id`) — мінімізація rebalance при рестарті
- Consumer lag — відстеження відставання consumer-а від producer-а
- `INSTANCE_ID` env var — ідентифікація конкретного екземпляра в логах
- `GET /api/notifications/partitions` — новий endpoint для перегляду призначених партицій

---

## Що змінилося порівняно з branch03

- Кількість `notification-service`: 1 → **3 екземпляри** (notification-service-1/2/3)
- Кожен екземпляр читає свою партицію (1:1 при 3 consumers / 3 partitions)
- Явно задано `partition.assignment.strategy: RangeAssignor`
- Новий env var `INSTANCE_ID` — передається кожному екземпляру окремо
- Логи тепер включають `instanceId`: `Instance: notification-service-2`
- `[REBALANCE]` логи при `ASSIGNED` / `REVOKED` подіях
- Новий endpoint `GET /api/notifications/partitions` — показує призначені партиції
- `/api/notifications/count` тепер повертає `instanceId` у відповіді
- Порти: notification-service-1=8082, notification-service-2=8083, notification-service-3=8084

---

## Архітектура

```
POST /api/orders
        │
        ▼
┌───────────────┐   04.orders.created (3 partitions)
│ order-service │──────────────────────────────────┐
│ :8081         │                                  │
└───────────────┘   04.orders.cancelled (1 part.)  │
                ───────────────────────────────────┼─────┐
                                                   │     │
                              ┌────────────────────┼─────┘
                              │                    │
                     partition-0           partition-1    partition-2
                              │                    │           │
                              ▼                    ▼           ▼
                    ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
                    │notification  │  │notification  │  │notification  │
                    │service-1     │  │service-2     │  │service-3     │
                    │:8082         │  │:8083         │  │:8084         │
                    └──────────────┘  └──────────────┘  └──────────────┘
                    └──────────────────────────────────────────────────┘
                                  notification-service-group

                              ┌──────────────────────┐
                              │      Apache Kafka     │
                              │      kafka:9092       │
                              └──────────────────────┘
                                          │
                              ┌──────────────────────┐
                              │      Kafka UI         │
                              │      :8080            │
                              └──────────────────────┘
```

### Топіки та їх налаштування

- **`04.orders.created`** — 3 партиції, retention 7 днів —
  нові замовлення, key=userId, читається всіма 3 instances
- **`04.orders.cancelled`** — 1 партиція, retention 7 днів —
  скасування, key=userId, читається тільки notification-service-1 (RangeAssignor)
- **`04.payments.processed`** — 3 партиції, retention 7 днів — резерв для branch05+
- **`04.notifications.sent`** — 1 партиція, retention 1 день — резерв для branch05+

> При `RangeAssignor` та нерівній кількості партицій — перший consumer отримує більше.
> `notification-service-1` читає `orders.cancelled[0]` + `orders.created[0]`.

---

## Ключові концепції цієї гілки

### Consumer Group і правило 1 partition = 1 consumer

Kafka гарантує: **одна партиція читається рівно одним consumer-ом у групі одночасно**.
Надлишкові consumers (більше ніж партицій) простоюють.

```
3 partitions + 3 consumers → 1:1 (оптимально)

notification-service-1 → orders.created[0]
notification-service-2 → orders.created[1]
notification-service-3 → orders.created[2]

3 partitions + 4 consumers → один простоює:
notification-service-4 → (idle, немає партиції)
```

### RangeAssignor vs RoundRobinAssignor

```
Топіки: orders.created (3 partitions), orders.cancelled (1 partition)
Consumers: C1, C2, C3

RangeAssignor (branch04):
  Розподіляє партиції кожного топіку окремо, зліва направо:
  C1 → orders.created[0], orders.cancelled[0]   ← бере залишок
  C2 → orders.created[1]
  C3 → orders.created[2]
  Мінус: C1 завантажений більше при непарній кількості партицій

RoundRobinAssignor:
  Об'єднує всі партиції всіх топіків, роздає по черзі:
  C1 → orders.created[0], orders.created[2]
  C2 → orders.created[1], orders.cancelled[0]
  C3 → (немає партицій якщо тільки 4 всього)
  Краще при багатьох топіках з різною кількістю партицій
```

### Rebalance

Rebalance — процес перерозподілу партицій при зміні складу consumer group.
**Eager rebalance (RangeAssignor):** всі consumers зупиняють читання, отримують нові призначення.

```
Initial state:
[ASSIGNED] notification-service-1 → [orders.cancelled[0], orders.created[0]]
[ASSIGNED] notification-service-2 → [orders.created[1]]
[ASSIGNED] notification-service-3 → [orders.created[2]]

Після зупинки notification-service-3:
[REVOKED]  notification-service-1 → відкликано всі партиції
[REVOKED]  notification-service-2 → відкликано всі партиції
[ASSIGNED] notification-service-1 → [orders.cancelled[0], orders.created[0]]
[ASSIGNED] notification-service-2 → [orders.created[1], orders.created[2]]
```

### Static Group Membership (group.instance.id)

```yaml
spring:
  kafka:
    consumer:
      properties:
        group.instance.id: notification-service-1
```

**Без static membership:**
- Consumer зупиняється → після `session.timeout.ms` (45с) Kafka робить rebalance
- Consumer повертається → отримує можливо інші партиції

**Зі static membership:**
- Consumer зупиняється → Kafka чекає `session.timeout.ms`
- Consumer повертається за цей час → ті самі партиції, **без rebalance**
- Важливо для Kubernetes rolling deploy та коротких рестартів

> У branch04 `group.instance.id` НЕ використовується через обмеження Spring Kafka:
> два `@KafkaListener` в одному сервісі → два consumers → однаковий `group.instance.id`
> → `FencedInstanceIdException`. Вирішення — в branch08.

---

## Як запустити

```bash
docker compose -f docker-compose-04.yml up --build
```

Перевірити готовність (6 контейнерів):

```bash
docker compose -f docker-compose-04.yml ps
# kafka, kafka-ui, order-service-b04
# notification-service-1-b04, notification-service-2-b04, notification-service-3-b04
```

---

## Як протестувати

### 1. Перевірити призначені партиції кожного екземпляра

```bash
curl -s http://localhost:8082/api/notifications/partitions | jq
curl -s http://localhost:8083/api/notifications/partitions | jq
curl -s http://localhost:8084/api/notifications/partitions | jq
```

або у Windows OS:
```cmd
curl -s http://localhost:8082/api/notifications/partitions
curl -s http://localhost:8083/api/notifications/partitions
curl -s http://localhost:8084/api/notifications/partitions
```

Очікувані відповіді:

```json
{"instanceId":"notification-service-1","assignedPartitions":["orders.cancelled[0]","orders.created[0]"],"count":2}
{"instanceId":"notification-service-2","assignedPartitions":["orders.created[1]"],"count":1}
{"instanceId":"notification-service-3","assignedPartitions":["orders.created[2]"],"count":1}
```

> `notification-service-1` отримує більше через `RangeAssignor`:
> `orders.cancelled` (1 partition) призначається першому consumer-у.

### 2. Надіслати замовлення та побачити розподіл

```bash
for user in user-01 user-02 user-03 user-04 user-05 user-06; do
  curl -s -X POST http://localhost:8081/api/orders \
    -H "Content-Type: application/json" \
    -d "{\"userId\":\"$user\",\"product\":\"Book\",\"quantity\":1,\"totalAmount\":20.00}" \
    | jq -r '"Order for \(.userId) → orderId: \(.orderId)"'
done
```

або у Windows OS:
```cmd
curl -s -X POST http://localhost:8081/api/orders ^
  -H "Content-Type: application/json" ^
  -d "{\"userId\":\"user-01\",\"product\":\"Book\",\"quantity\":1,\"totalAmount\":20.00}"
```

Дивимося в логи — кожен екземпляр обробив свою партицію:

```bash
docker logs notification-service-1-b04 --tail=20
docker logs notification-service-2-b04 --tail=20
docker logs notification-service-3-b04 --tail=20
```

Очікуваний лог `notification-service-2`:

```
╔══════════════════════════════════════════╗
║  ORDER CREATED  #2
║  Instance  : notification-service-2
║  Key: user-03  →  Partition: 1  Offset: 2
║  Order ID  : ...
║  User ID   : user-03
╚══════════════════════════════════════════╝
```

### 3. Продемонструвати rebalance — зупиняємо один екземпляр

```bash
docker stop notification-service-3-b04
```

Чекаємо ~10–15 секунд. Перевіряємо перерозподіл:

```bash
curl -s http://localhost:8082/api/notifications/partitions | jq
curl -s http://localhost:8083/api/notifications/partitions | jq
```

Очікуваний результат після rebalance:

```json
{"instanceId":"notification-service-1","assignedPartitions":["orders.cancelled[0]","orders.created[0]"],"count":2}
{"instanceId":"notification-service-2","assignedPartitions":["orders.created[1]","orders.created[2]"],"count":2}
```

`notification-service-2` тепер читає 2 партиції замість 1.

### 4. Відновлення — запускаємо назад

```bash
docker start notification-service-3-b04
```

Логи покажуть rebalance і повернення до рівномірного розподілу.

### 5. CLI моніторинг consumer group

```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group notification-service-group
```

Очікуваний вивід (3 active members):

```
GROUP                       TOPIC              PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
notification-service-group  04.orders.created  0          6               6               0
notification-service-group  04.orders.created  1          4               4               0
notification-service-group  04.orders.created  2          5               5               0
```

### 6. Kafka UI

Відкрити: [http://localhost:8080](http://localhost:8080)

- `Consumer Groups` → `notification-service-group` → вкладка **Members**
- Видно 3 members, кожен з призначеними партиціями
- Після зупинки одного → 2 members, один читає 2 партиції

---

## Як це працює всередині

### application.yml (notification-service) — що змінилося

```yaml
# branch03 (було):
spring:
  kafka:
    consumer:
      group-id: notification-service-group
      # assignment strategy не задано (default)

# branch04 (стало):
spring:
  kafka:
    consumer:
      group-id: notification-service-group
      properties:
        partition.assignment.strategy: org.apache.kafka.clients.consumer.RangeAssignor

instance:
  id: ${INSTANCE_ID:notification-service-1}
```

### OrderEventListener.kt — що змінилося

```kotlin
// branch03: просто @KafkaListener
class OrderEventListener { ... }

// branch04: реалізує ConsumerSeekAware для відстеження rebalance
class OrderEventListener(
    @Value("\${instance.id}") private val instanceId: String
) : ConsumerSeekAware {

    override fun onPartitionsAssigned(assignments, callback) {
        log.info("[ASSIGNED] Instance [{}] ASSIGNED: {}", instanceId,
            assignments.keys.map { "${it.topic()}[${it.partition()}]" })
    }

    override fun onPartitionsRevoked(partitions) {
        log.info("[REVOKED] Instance [{}] REVOKED: {}", instanceId,
            partitions.map { "${it.topic()}[${it.partition()}]" })
    }
}
```

`ConsumerSeekAware` — Spring Kafka інтерфейс.
Методи `onPartitionsAssigned` / `onPartitionsRevoked` викликаються автоматично при кожному rebalance.

### NotificationController.kt — новий endpoint

```kotlin
@GetMapping("/partitions")
fun assignedPartitions(): Map<String, Any> {
    val parts = listener.getAssignedPartitions()
        .map { "${it.topic()}[${it.partition()}]" }.sorted()
    return mapOf(
        "instanceId" to instanceId,
        "assignedPartitions" to parts,
        "count" to parts.size
    )
}
```

### docker-compose-04.yml — три екземпляри одного образу

```yaml
notification-service-1:
  build: branch04_customer_groups/notification-service
  container_name: notification-service-1-b04
  ports:
    - "8082:8082"
  environment:
    - INSTANCE_ID=notification-service-1

notification-service-2:
  build: branch04_customer_groups/notification-service
  container_name: notification-service-2-b04
  ports:
    - "8083:8082"         # зовнішній 8083 → внутрішній 8082
  environment:
    - INSTANCE_ID=notification-service-2

notification-service-3:
  build: branch04_customer_groups/notification-service
  container_name: notification-service-3-b04
  ports:
    - "8084:8082"         # зовнішній 8084 → внутрішній 8082
  environment:
    - INSTANCE_ID=notification-service-3
```

Всі три збирають **один і той самий Docker-образ** — `INSTANCE_ID` передається через env var.

---

## Структура проєкту (зміни відносно branch03)

```
kafka-laboratory/
├── docker-compose-04.yml                      ← 3x notification-service, порти 8082/8083/8084
├── branch04_customer_groups/
│   ├── notification-service/
│   │   └── src/main/kotlin/com/kafkalab/notification/
│   │       ├── controller/NotificationController.kt  ← NEW: instanceId + GET /partitions
│   │       └── listener/OrderEventListener.kt        ← ConsumerSeekAware + instanceId в логах
│   │   └── src/main/resources/
│   │       └── application.yml                       ← RangeAssignor, INSTANCE_ID
│   └── order-service/
│       └── src/main/kotlin/com/kafkalab/order/
│           └── config/KafkaTopicConfig.kt            ← топіки 04.orders.*
```

---

## Що далі — branch05

У наступній гілці вивчаємо **Multiple Consumer Groups**:
- Два незалежних сервіси читають **один і той самий** топік
- `notification-service-group` та `analytics-service-group` — кожна з власним offset
- Зупинити `analytics-service`, опублікувати 10 подій → analytics дочитає пропущене при старті
- Демонстрація "replay": нова consumer group читає топік з початку

---
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------

## Слайди для презентації

**Слайд 1: Заголовок**
- Branch 04 — Consumer Groups
- Як Kafka розподіляє партиції між екземплярами сервісу

---

**Слайд 2: Проблема branch03**
- 1 notification-service читає всі 3 партиції
- Throughput обмежений швидкістю одного consumer-а
- При збої — все читання зупиняється
- Рішення: запустити кілька екземплярів — Consumer Group

---

**Слайд 3: Правило Consumer Group**
- Одна партиція → тільки один consumer у групі одночасно
- 3 партиції + 3 consumers → кожен читає 1 партицію (1:1)
- 3 партиції + 4 consumers → один простоює (зайвий)
- 3 партиції + 2 consumers → один читає 2 партиції

---

**Слайд 4: RangeAssignor**
- Сортує партиції кожного топіку окремо
- Розподіляє зліва направо по consumers
- C1 → orders.created[0] + orders.cancelled[0]
- C2 → orders.created[1], C3 → orders.created[2]
- Мінус: перший consumer бере залишок (нерівномірно)

---

**Слайд 5: Rebalance — що це**
- Rebalance = перерозподіл партицій при зміні складу групи
- Тригери: новий consumer підключився / consumer відключився (crash або graceful)
- Eager rebalance: всі зупиняються → отримують нові призначення → продовжують читання
- Stop-the-world пауза → Consumer lag зростає

---

**Слайд 6: Rebalance — демо**
- Зупиняємо notification-service-3
- Kafka чекає session.timeout.ms (~45с)
- notification-service-2 отримує partition-2 додатково
- Запускаємо знову → ще один rebalance → рівний розподіл

---

**Слайд 7: Static Group Membership**
- group.instance.id = notification-service-1
- Consumer повертається до session.timeout → ті ж партиції без rebalance
- Важливо для Kubernetes rolling deploy
- У branch04 НЕ використовується через FencedInstanceIdException (2 listeners)
- Вирішення: branch08

---

**Слайд 8: ConsumerSeekAware**
- Spring Kafka інтерфейс для відстеження partition assignment
- onPartitionsAssigned() → логуємо [ASSIGNED]
- onPartitionsRevoked() → логуємо [REVOKED]
- Дозволяє реагувати на rebalance: зберегти стан, flush буфер

---

**Слайд 9: INSTANCE_ID env var**
- Один Docker-образ → три екземпляри з різними INSTANCE_ID
- notification-service-1/2/3 передається через environment
- application.yml: instance.id = ${INSTANCE_ID:notification-service-1}
- GET /api/notifications/partitions → показує instanceId + список партицій

---

**Слайд 10: Consumer Lag моніторинг**
- kafka-consumer-groups --describe --group notification-service-group
- Колонки: PARTITION, CURRENT-OFFSET, LOG-END-OFFSET, LAG, CONSUMER-ID
- LAG = LOG-END-OFFSET - CURRENT-OFFSET
- LAG > 0 → consumer відстає → потрібен моніторинг (branch17)

---

**Слайд 11: Ключові висновки**
- Consumer Group = горизонтальне масштабування читання
- 1 partition = 1 consumer (ніколи не більше в одній групі)
- Rebalance — неминучий при зміні складу групи, але можна мінімізувати
- RangeAssignor — default у Spring Kafka, нерівномірний при різних топіках
- Static Membership — менше rebalance для стабільних consumer-ів
- Consumer lag — ключова метрика продуктивності

---

**Слайд 12: Що далі — Multiple Consumer Groups**
- branch05: два незалежних сервіси читають один топік
- notification-group та analytics-group — окремі offsets
- Replay: нова group читає з earliest
- Незалежне масштабування кожної групи

---

## Текст для презентації (скрипт)

**Слайд 1:**
Вітаю! У цій гілці ми вирішуємо реальну проблему масштабування.
У branch03 у нас один notification-service читав всі три партиції.
Це означає, що весь throughput обмежений швидкістю одного процесу.
Сьогодні запустимо три екземпляри і побачимо, як Kafka розподіляє роботу між ними.

**Слайд 2:**
Проблема branch03 проста: один consumer — один потік обробки.
Якщо в нас 3 партиції, але 1 consumer — ми не використовуємо паралелізм Kafka.
Consumer Group — це механізм, який дозволяє кільком екземплярам одного сервісу
читати різні партиції одночасно.

**Слайд 3:**
Головне правило consumer group: одна партиція читається тільки одним consumer-ом у групі.
Це залізна гарантія Kafka.
Маємо 3 партиції і 3 consumers — ідеальний розподіл 1:1.
Якщо consumers більше ніж партицій — надлишкові просто чекають.
Якщо менше — один consumer читає кілька партицій.

**Слайд 4:**
Стратегія RangeAssignor — це default у Spring Kafka.
Вона бере кожен топік окремо, сортує партиції і розподіляє зліва направо.
Але є нюанс: якщо кількість партицій не ділиться рівно на кількість consumers —
перший consumer завжди бере залишок.
У нашому випадку notification-service-1 читає і orders.cancelled, і orders.created[0].

**Слайд 5:**
Rebalance — це процес перерозподілу партицій при зміні складу групи.
Він відбувається коли новий consumer підключається або існуючий відключається.
Eager rebalance, який використовує RangeAssignor: всі consumers спочатку зупиняють читання,
потім Kafka перерозподіляє партиції і всі починають знову.
Це "stop the world" пауза — під час неї consumer lag може зростати.

**Слайд 6:**
Давайте подивимося на rebalance на практиці.
Зупиняємо notification-service-3 командою docker stop.
Kafka чекає session.timeout.ms — це 45 секунд за замовчуванням.
Після цього notification-service-2 отримує partition-2 додатково до своєї partition-1.
Запускаємо сервіс назад — відбувається ще один rebalance і розподіл повертається до 1:1.

**Слайд 7:**
Static Group Membership — це оптимізація для зменшення кількості rebalance.
Якщо задати group.instance.id, Kafka "пам'ятає" конкретний consumer за цим ідентифікатором.
Якщо consumer перезапускається і повертається до закінчення session.timeout —
він отримує назад ті самі партиції без повного rebalance.
Це особливо важливо для Kubernetes, де pods часто перезапускаються при rolling deploy.

**Слайд 8:**
ConsumerSeekAware — це Spring Kafka інтерфейс для відстеження змін partition assignment.
Реалізуємо два методи: onPartitionsAssigned викликається коли consumer отримує нові партиції,
onPartitionsRevoked — коли партиції відкликаються перед rebalance.
Це дозволяє логувати події rebalance і реагувати на них:
наприклад, зберегти поточний стан або flush буфер перед втратою партиції.

**Слайд 9:**
Для ідентифікації кожного екземпляра ми використовуємо INSTANCE_ID env var.
Один Docker-образ запускається тричі з різними значеннями: notification-service-1, 2, 3.
application.yml читає це значення через Spring placeholder.
Новий endpoint GET /api/notifications/partitions показує,
які партиції зараз призначені конкретному екземпляру — зручно для дебагінгу.

**Слайд 10:**
Consumer lag — ключова метрика для моніторингу consumer groups.
Команда kafka-consumer-groups --describe показує для кожної партиції:
поточний offset consumer-а, останній offset в партиції і різницю — lag.
Якщо lag постійно зростає — consumer не встигає за producer-ом.
У branch17 ми підключимо Grafana і будемо моніторити lag в реальному часі.

**Слайд 11:**
Підсумуємо. Consumer Group — це горизонтальне масштабування читання в Kafka.
Головне правило: одна партиція читається тільки одним consumer-ом у групі.
Rebalance неминучий при зміні складу групи, але static membership може його мінімізувати.
RangeAssignor — default, але не завжди рівномірний.
Consumer lag — перший сигнал що система не справляється з навантаженням.

**Слайд 12:**
У наступній гілці, branch05, ми побачимо іншу сторону consumer groups.
Два різних сервіси будуть читати один і той самий топік незалежно.
notification-service-group і analytics-service-group — кожна зі своїм offset.
Важлива властивість: зупинити один сервіс, накопичити повідомлення,
запустити знову — він дочитає все пропущене. До зустрічі!

---

## Тестові питання

**Питання 1:** Скільки consumers у групі може одночасно читати одну партицію?

A) Необмежено
B) Рівно один
C) Залежить від кількості partition replicas
D) Два, якщо обидва мають однаковий group.instance.id

**Відповідь:** B — Основна гарантія Kafka: одна партиція читається рівно одним consumer-ом
у групі одночасно. Це фундаментальна властивість Consumer Group.

---

**Питання 2:** У групі 3 consumers і топік з 3 партиціями.
Що станеться якщо додати четвертий consumer?

A) Четвертий consumer читатиме всі 3 партиції паралельно
B) Четвертий consumer буде idle — йому нема партиції
C) Kafka автоматично збільшить кількість партицій до 4
D) Стане помилка — не можна мати більше consumers ніж партицій

**Відповідь:** B — Kafka не може призначити партицію consumer-у якщо всі вже зайняті.
Четвертий consumer просто чекає, поки не відключиться хтось із трьох.

---

**Питання 3:** Що таке Rebalance у Kafka?

A) Перерозподіл повідомлень між топіками
B) Процес перерозподілу партицій між consumers у групі при зміні її складу
C) Автоматичне збільшення кількості партицій при зростанні навантаження
D) Синхронізація offsets між різними consumer groups

**Відповідь:** B — Rebalance відбувається коли consumer приєднується до групи або покидає її.
Kafka перерозподіляє партиції між активними members.

---

**Питання 4:** Чим RangeAssignor відрізняється від RoundRobinAssignor?

A) RangeAssignor швидший, RoundRobinAssignor точніший
B) RangeAssignor розподіляє партиції кожного топіку окремо;
   RoundRobinAssignor об'єднує всі партиції всіх топіків і роздає по черзі
C) RoundRobinAssignor доступний тільки в Confluent Kafka
D) RangeAssignor використовується тільки для одного топіка

**Відповідь:** B — Ключова різниця в підході.
RangeAssignor може дати першому consumer-у більше при непарній кількості партицій.
RoundRobinAssignor рівномірніший при кількох топіках.

---

**Питання 5:** Що таке Consumer Lag?

A) Затримка між відправкою повідомлення і його отриманням producer-ом
B) Різниця між останнім offset у партиції і поточним offset consumer-а
C) Час очікування нового повідомлення при порожній партиції
D) Кількість повідомлень у буфері producer-а

**Відповідь:** B — Consumer Lag = `LOG-END-OFFSET - CURRENT-OFFSET`.
Показує скільки повідомлень consumer ще не прочитав. Зростаючий lag — сигнал проблем.

---

**Питання 6:** Навіщо потрібен `group.instance.id` (Static Group Membership)?

A) Для призначення фіксованої партиції конкретному consumer-у назавжди
B) Щоб consumer після короткого рестарту отримав назад ті самі партиції без full rebalance
C) Для шифрування трафіку між consumer-ом і брокером
D) Для ідентифікації consumer у логах Kafka

**Відповідь:** B — Static Membership дозволяє Kafka "пам'ятати" consumer за його instance ID.
Якщо він повертається до `session.timeout.ms` — отримує ті самі партиції, rebalance не відбувається.

---

**Питання 7:** Яка команда показує поточний стан consumer group (offsets, lag, members)?

A) `kafka-topics --describe --group notification-service-group`
B) `kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group notification-service-group`
C) `kafka-configs --describe --entity-type groups`
D) `kafka-log-dirs --bootstrap-server localhost:9092 --group notification-service-group`

**Відповідь:** B — `kafka-consumer-groups --describe` показує для кожної партиції:
CURRENT-OFFSET, LOG-END-OFFSET, LAG і ідентифікатор consumer-а який її читає.

---

**Питання 8:** Три partition, два consumers з `RangeAssignor`. Як розподіляться партиції?

A) C1 → p0, p1; C2 → p2
B) C1 → p0; C2 → p1, p2
C) C1 → p0, p2; C2 → p1
D) C1 → p0, p1, p2; C2 → idle

**Відповідь:** A — RangeAssignor сортує партиції і ділить: 3 партиції / 2 consumers = 1.5,
тобто перший отримує ceil(1.5)=2. Результат: C1→[p0, p1], C2→[p2].

---

**Питання 9:** Що означає "Eager Rebalance" у Kafka?

A) Rebalance відбувається негайно без очікування підтвердження від consumers
B) Всі consumers спочатку відкликають свої партиції, потім отримують нові призначення
C) Тільки "зайві" партиції переміщуються між consumers без зупинки
D) Rebalance відбувається тільки при додаванні нового consumer, не при видаленні

**Відповідь:** B — Eager rebalance (RangeAssignor, RoundRobinAssignor): спочатку REVOKED для всіх,
потім ASSIGNED. Це "stop the world" — під час цього всі consumers призупиняють обробку.

---

**Питання 10:** Consumer в групі не отримав жодної партиції. Яка причина?

A) Consumer не підключився до правильного bootstrap-server
B) Consumers у групі вже більше ніж партицій у топіку
C) group-id не вказано в конфігурації
D) Consumer використовує невірний deserializer

**Відповідь:** B — Якщо consumers більше ніж партицій, надлишковим просто нема що читати.
Вони залишаються в групі і стають активними при наступному rebalance
(наприклад, якщо хтось відключиться).