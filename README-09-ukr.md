# Branch 09 — Error Handling & Dead Letter Topic

## `branch09_error_retry` — що вивчаємо

- **`@RetryableTopic`** — анотація Spring Kafka для non-blocking retry через окремі Kafka топіки.
- **Non-blocking retry** — невдалі повідомлення переміщуються до retry-топіків; основний топік читається далі.
- **Retry топіки** — `09.orders.created-retry-0`, `09.orders.created-retry-1` (автоматично створюються).
- **Dead Letter Topic (DLT)** — `09.orders.created-dlt` — кінцева точка для повідомлень після N невдалих спроб.
- **`@DltHandler`** — Spring Kafka метод для обробки DLT повідомлень (логування, alerting).
- **Exponential backoff** — `delay=1000ms, multiplier=5.0` → retry через 1с, потім через 5с.
- **Retry headers** — Kafka headers з metadata: оригінальний топік, партиція, exception message.
- **Blocking vs Non-blocking retry** — порівняння: spring-retry (in-memory) vs retry topics (Kafka-persistent).
- **Новий сервіс `payment-service`** — демонструє retry для критичних транзакцій.
- **`PaymentException`** — кастомний RuntimeException для симуляції збою платежу.

## Що змінилося порівняно з branch08

- Замість `notification-service` основний consumer тепер **`payment-service`**.
- Доданий `@RetryableTopic` з 3 спробами і exponential backoff (1s → 5s).
- Автоматично створено 3 нових топіки: `retry-0`, `retry-1`, `dlt`.
- Доданий `@DltHandler` для обробки повідомлень після вичерпання retry.
- Новий ендпоінт `POST /api/payments/simulate-failure/{count}` для демо.
- Новий ендпоінт `GET /api/payments/stats` — статистика processed/dlt/failNextN.
- Notification-service видалений з цієї гілки — фокус на error handling.
- Назви топіків змінено на `09.orders.*`.

## Архітектура

```
order-service-b09 :8081
  POST /api/orders → 09.orders.created
          │
          ▼
 payment-service-b09 :8083
 @RetryableTopic(attempts=3)
          │
  ┌───────┼───────────────────────────────────────────────────┐
  │       │  Attempt 1 (t=0)                                   │
  │  09.orders.created ──── success ──────────────────────► OK │
  │       │                                                     │
  │       │  failNextN > 0 → PaymentException                  │
  │       ▼                                                     │
  │  09.orders.created-retry-0 (delay 1s)                      │
  │       │  failNextN > 0 → PaymentException                  │
  │       ▼                                                     │
  │  09.orders.created-retry-1 (delay 5s)                      │
  │       │  failNextN > 0 → PaymentException                  │
  │       ▼                                                     │
  │  09.orders.created-dlt  → @DltHandler ← MANUAL REVIEW      │
  └───────────────────────────────────────────────────────────┘
```

### Топіки та їх налаштування

- `09.orders.created` → партиції: 3, retention: 7 днів, основний топік.
- `09.orders.created-retry-0` → auto-created, затримка 1с перед обробкою.
- `09.orders.created-retry-1` → auto-created, затримка 5с перед обробкою.
- `09.orders.created-dlt` → auto-created, кінцева точка після 3 невдалих спроб.

## Ключові концепції цієї гілки

### Проблема без DLT

```
Consumer читає offset=42 → RuntimeException при обробці
  Варіант А: retry indefinitely → consumer застряє на offset=42, весь топік блокується
  Варіант Б: log & skip → повідомлення втрачено без жодного запису
  Варіант В: catch всі exceptions → обробка "успішна" незалежно від результату

Жоден з варіантів не підходить для production.
```

### Non-blocking Retry через Retry Topics

```
Attempt 1 (t=0ms):    topic: 09.orders.created, offset=42
  → PaymentException ✗
  → повідомлення переміщується до retry-0 (з header backoff_next_elapse)

Attempt 2 (t=1000ms): topic: 09.orders.created-retry-0
  → PaymentException ✗
  → повідомлення переміщується до retry-1

Attempt 3 (t=6000ms): topic: 09.orders.created-retry-1
  → PaymentException ✗
  → повідомлення переміщується до dlt

@DltHandler:          topic: 09.orders.created-dlt
  → логування, alerting, manual review
```

**Non-blocking**: поки `offset=42` очікує в retry-0 через 1 секунду,
offset=43, 44, 45 читаються з `09.orders.created` і обробляються нормально.

### @RetryableTopic анотація

```kotlin
// OrderPaymentListener.kt
@RetryableTopic(
    attempts = "3",               // 1 основна + 2 retry = 3 спроби всього
    backoff = Backoff(
        delay = 1000,             // перший retry через 1000мс
        multiplier = 5.0          // другий retry через 1000 * 5 = 5000мс
    ),
    topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,  // -retry-0, -retry-1
    dltStrategy = DltStrategy.FAIL_ON_ERROR
)
@KafkaListener(topics = ["09.orders.created"], groupId = "payment-service-group")
fun handleOrderCreated(
    record: ConsumerRecord<String, OrderCreatedEvent>,
    @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String
) {
    if (failNextN.get() > 0) {
        failNextN.decrementAndGet()
        throw PaymentException("Simulated payment failure for order ${record.value().orderId}")
    }
    // ... обробка платежу ...
}

@DltHandler
fun handleDlt(
    record: ConsumerRecord<String, OrderCreatedEvent>,
    @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String
) {
    dltCount.incrementAndGet()
    log.error("[DLT] Order {} → Manual intervention required!", record.value().orderId)
}
```

### Retry Headers у Kafka

Кожне повідомлення в retry/dlt топіку несе headers:

```
kafka_original_topic:      09.orders.created
kafka_original_partition:  0
kafka_original_offset:     42
kafka_exception-message:   Simulated payment failure for order abc-123
kafka_backoff_next_elapse: 1725000000000  ← timestamp коли обробляти
```

### Blocking vs Non-blocking Retry

```
Blocking (Spring Retry / DefaultErrorHandler):
  offset=42 падає → retry in-memory → весь consumer чекає
  throughput: ЗУПИНЯЄТЬСЯ до вичерпання retry
  retry state: в пам'яті → втрачається при рестарті
  visibility: прихований (нема в Kafka UI)

Non-blocking (@RetryableTopic):
  offset=42 падає → переміщується до retry-topic
  основний топік: продовжує читатися (offset=43, 44, ...)
  retry state: в Kafka → зберігається при рестарті
  visibility: видно в Kafka UI як окремі топіки
```

## Як запустити

```bash
docker compose -f docker-compose-09.yml up --build
```

Перевірити 4 контейнери:

```bash
docker compose -f docker-compose-09.yml ps
# kafka, kafka-ui, order-service-b09, payment-service-b09
```

## Як протестувати

### 1. Успішна обробка (без збоїв)

```bash
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-01","product":"Book","quantity":1,"totalAmount":25.00}'

curl http://localhost:8083/api/payments/stats
# {"processed":1,"dlt":0,"failNextN":0}
```

### 2. Головний демо-сценарій: retry → DLT

```bash
# Крок 1: Запланувати 3 збої (одне повідомлення пройде всі спроби і потрапить у DLT)
curl -s -X POST http://localhost:8083/api/payments/simulate-failure/3
# {"scheduledFailures":3,"message":"Next 3 invocations will throw PaymentException → DLT"}

# Крок 2: Надіслати замовлення
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-fail","product":"Doomed Order","quantity":1,"totalAmount":99.00}'

# Логи payment-service:
# [RETRY] Attempt on topic=09.orders.created, orderId=abc-123
# (через 1с)
# [RETRY] Attempt on topic=09.orders.created-retry-0, orderId=abc-123
# (через 5с)
# [RETRY] Attempt on topic=09.orders.created-retry-1, orderId=abc-123
# [DLT] ⚠ Order abc-123 → Manual intervention required!

# Крок 3: Перевірити статистику
curl http://localhost:8083/api/payments/stats
# {"processed":0,"dlt":1,"failNextN":0}
```

**Windows (PowerShell):**

```powershell
Invoke-RestMethod -Method POST -Uri http://localhost:8083/api/payments/simulate-failure/3
Invoke-RestMethod -Method POST -Uri http://localhost:8081/api/orders `
  -ContentType "application/json" `
  -Body '{"userId":"user-fail","product":"Doomed Order","quantity":1,"totalAmount":99.00}'
```

### 3. Partial retry — успіх після 1 спроби

```bash
# 1 збій → повідомлення обробиться на retry-0
curl -s -X POST http://localhost:8083/api/payments/simulate-failure/1

curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-lucky","product":"Lucky Order","quantity":1,"totalAmount":50.00}'

# Логи:
# [RETRY] Attempt on topic=09.orders.created → fail
# (через 1с)
# ╔═══ PAYMENT PROCESSED #1 (topic=09.orders.created-retry-0) ╗
```

### 4. Kafka UI — перевірити retry топіки

Відкрити: `http://localhost:8080`

- `Topics` → бачимо 4 топіки:
  - `09.orders.created`
  - `09.orders.created-retry-0`
  - `09.orders.created-retry-1`
  - `09.orders.created-dlt`
- `09.orders.created-dlt` → Messages → бачимо Headers з retry metadata.

### 5. Перевірити retry headers через CLI

```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic 09.orders.created-dlt \
  --from-beginning \
  --property print.headers=true \
  --max-messages 1
```

## Як це працює всередині

### Producer (без змін порівняно з branch08)

`order-service` публікує у `09.orders.created` з ключем `userId`.

### Consumer — payment-service (НОВИЙ)

```kotlin
// OrderPaymentListener.kt — ключові частини

@RetryableTopic(attempts = "3", backoff = Backoff(delay = 1000, multiplier = 5.0),
    topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
    dltStrategy = DltStrategy.FAIL_ON_ERROR)
@KafkaListener(topics = ["09.orders.created"], groupId = "payment-service-group")
fun handleOrderCreated(
    record: ConsumerRecord<String, OrderCreatedEvent>,
    @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String
) {
    val event = record.value()
    if (failNextN.get() > 0) {
        failNextN.decrementAndGet()
        log.warn("[RETRY] Attempt on topic={}, orderId={}", topic, event.orderId)
        throw PaymentException("Simulated payment failure for order ${event.orderId}")
    }
    processedCount.incrementAndGet()
    log.info("╔══ PAYMENT PROCESSED #{}", processedCount.get())
    log.info("║  Topic: {}  Partition: {}  Offset: {}", topic, record.partition(), record.offset())
}

@DltHandler
fun handleDlt(record: ConsumerRecord<String, OrderCreatedEvent>,
              @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String) {
    dltCount.incrementAndGet()
    log.error("║  ⚠ DEAD LETTER TOPIC  ⚠")
    log.error("║  Order ID: {}  → Manual intervention required!", record.value().orderId)
}
```

```yaml
# payment-service/application.yml
server:
  port: 8083
spring:
  kafka:
    consumer:
      group-id: payment-service-group
      auto-offset-reset: earliest
```

## Структура проєкту (зміни відносно branch08)

```
kafka-laboratory/
├── docker-compose-09.yml                              ← order + payment (без notification)
├── branch09_error_retry/
│   ├── order-service/                                 ← топік 09.orders.created
│   └── payment-service/                               ← NEW сервіс
│       └── src/main/kotlin/.../listener/
│           └── OrderPaymentListener.kt                ← @RetryableTopic, @DltHandler
│       └── src/main/kotlin/.../controller/
│           └── PaymentController.kt                   ← /stats, /simulate-failure
│       └── src/main/kotlin/.../exception/
│           └── PaymentException.kt                    ← RuntimeException subclass
│       └── src/main/resources/
│           └── application.yml
└── README09.md
```

## Що далі — branch10

- **JSON Serialization** — `JsonSerializer` / `JsonDeserializer` зі Spring Kafka.
- **`spring.json.type.mapping`** — alias замість FQCN у `__TypeId__` header.
- **Event versioning** — поле `eventVersion` для backward compatibility.
- **Nested objects** — `List<OrderItem>` всередині `OrderCreatedEvent`.
- **Tri-service pipeline** — order → payment → notification через два топіки.

---
---

## Слайди для презентації (11 слайдів)

**Слайд 1: Branch 09 — Error Handling & Dead Letter Topic**
- Apache Kafka for Certification & Production
- @RetryableTopic, DLT, non-blocking retry

**Слайд 2: Agenda**
1. Проблема без error handling
2. Blocking vs Non-blocking retry
3. @RetryableTopic — конфігурація
4. Retry Topics — автоматичне створення
5. Exponential Backoff
6. @DltHandler
7. Retry Headers
8. Demo: retry → DLT
9. Key takeaways & CCDAK

**Слайд 3: Проблема без Error Handling**
- Варіант А: retry indefinitely → consumer застряє, топік блокується
- Варіант Б: log & skip → повідомлення втрачено назавжди
- Варіант В: catch all exceptions → обробка "успішна" незалежно від результату
- Жоден не підходить для production критичних сервісів

**Слайд 4: Blocking vs Non-blocking Retry**
- **Blocking**: retry in-memory, весь consumer чекає, стан втрачається при рестарті
- **Non-blocking**: retry через Kafka топіки, основний топік читається далі
- Non-blocking зберігає throughput і persistence retry state
- @RetryableTopic — Spring Kafka реалізація non-blocking

**Слайд 5: @RetryableTopic — Конфігурація**
- `attempts="3"`: 1 основна + 2 retry
- `delay=1000, multiplier=5.0`: backoff 1с → 5с
- `topicSuffixingStrategy`: суфікс -retry-0, -retry-1
- `dltStrategy=FAIL_ON_ERROR`: DLT handler може кидати exception

**Слайд 6: Retry Topics — Auto-Created**
- Spring Kafka автоматично створює retry і DLT топіки
- `09.orders.created-retry-0`: затримка 1с
- `09.orders.created-retry-1`: затримка 5с
- `09.orders.created-dlt`: кінцева точка
- Всі топіки видно в Kafka UI

**Слайд 7: @DltHandler**
- Викликається після N невдалих спроб (exhausted retries)
- Повідомлення НЕ буде повторено після DLT handler
- Use cases: логування, alert Slack/PagerDuty, запис у БД для manual review
- `dltStrategy=FAIL_ON_ERROR`: виключення в @DltHandler не замовчується

**Слайд 8: Retry Headers**
- Кожне повідомлення несе metadata про retry
- `kafka_original_topic`, `kafka_original_offset` — звідки прийшло
- `kafka_exception-message` — причина відмови
- `kafka_backoff_next_elapse` — timestamp коли обробляти
- Корисно для audit і debugging в Kafka UI

**Слайд 9: Demo — Retry → DLT**
- `simulate-failure/3` → 3 збої → одне повідомлення пройде всі 3 спроби → DLT
- Логи показують топік кожної спроби: created → retry-0 → retry-1 → dlt
- stats: processed=0, dlt=1
- Partial retry: `simulate-failure/1` → успіх на retry-0

**Слайд 10: Key Takeaways**
- Non-blocking retry через Kafka топіки — production стандарт
- @RetryableTopic = авто-створення retry/dlt + retry headers + @DltHandler
- Exponential backoff запобігає overload при тимчасовому збої
- DLT — mandatory для будь-якого критичного consumer
- Retry state в Kafka — переживає рестарт сервісу

**Слайд 11: What's Next — Branch 10: JSON Serialization**
- spring.json.type.mapping — alias замість FQCN
- Event versioning — backward compatibility
- Nested objects: List<OrderItem>
- 3-service pipeline: order → payment → notification

## Текст для презентації (скрипт)

**Слайд 1:**
Дев'ята гілка — Error Handling і Dead Letter Topic.
Це одна з найважливіших тем для production: що робити коли consumer не може обробити повідомлення?
Неправильний підхід може призвести до втрати даних або зупинки всього pipeline.

**Слайд 2:**
Пройдемо дев'ять тем від проблеми до рішення.
Головний інсайт: правильна обробка помилок — це архітектурне рішення, не просто try-catch.

**Слайд 3:**
Без error handling є три погані варіанти.
Перший: retry indefinitely — consumer застряє на одному повідомленні і не читає далі.
Другий: log and skip — дані втрачаються назавжди, ніхто не знає що сталося.
Третій: catch all — обробка "успішна" навіть коли платіж не пройшов.

**Слайд 4:**
Blocking retry — spring-retry або DefaultErrorHandler — тримає consumer заблокованим під час retry.
Non-blocking — @RetryableTopic — переміщує невдале повідомлення до окремого топіку.
Поки воно там чекає — основний топік читається далі.
Retry state зберігається в Kafka і переживає рестарт сервісу.

**Слайд 5:**
@RetryableTopic конфігурується прямо на listener-методі.
attempts=3 означає одна основна плюс дві retry спроби.
delay=1000ms і multiplier=5.0 дають exponential backoff: 1 секунда потім 5 секунд.
dltStrategy=FAIL_ON_ERROR означає що exception у @DltHandler не буде замовчано.

**Слайд 6:**
Spring Kafka автоматично створює retry і DLT топіки при першому запуску.
SUFFIX_WITH_INDEX_VALUE дає суфікси -retry-0 і -retry-1.
Всі чотири топіки видно в Kafka UI — це велика перевага над blocking retry.
Можна побачити скільки повідомлень застряло і на якому етапі.

**Слайд 7:**
@DltHandler — останній рубіж захисту.
Після вичерпання всіх retry повідомлення потрапляє сюди.
Тут потрібно: записати у базу даних для ручного розгляду, надіслати alert у Slack або PagerDuty.
dltStrategy=FAIL_ON_ERROR гарантує що exception у @DltHandler не буде тихо проігнорований.

**Слайд 8:**
Retry headers — metadata що додається до кожного повідомлення в retry топіку.
Вони дозволяють відстежити звідки прийшло повідомлення і чому відмовило.
kafka_backoff_next_elapse — точний timestamp коли consumer повинен спробувати знову.
Це "розклад" для retry scheduler.

**Слайд 9:**
У демо три сценарії.
Перший: три збої підряд — повідомлення проходить через retry-0, retry-1 і потрапляє в DLT.
Другий: один збій — успіх на retry-0, тобто повідомлення оброблено не відразу але успішно.
Третій: без збоїв — processed=1, dlt=0, все добре.

**Слайд 10:**
Основні висновки: @RetryableTopic — готове рішення для production error handling.
Non-blocking retry зберігає throughput під час тимчасових збоїв.
DLT — обов'язковий для будь-якого critical consumer.
Retry state в Kafka переживає рестарт на відміну від in-memory retry.

**Слайд 11:**
Наступна гілка — JSON Serialization.
Розберемо як безпечно передавати складні об'єкти між сервісами.
spring.json.type.mapping вирішує проблему ClassNotFoundException при cross-service десеріалізації.
І побудуємо тристоронній pipeline: order, payment і notification.

## Тестові питання (до 10 питань)

**Питання 1:**
`@RetryableTopic(attempts="3", backoff=@Backoff(delay=1000, multiplier=5.0))`.
Через скільки часу відбудеться третя спроба (з моменту першої відмови)?

A) 1000мс
B) 5000мс
C) 6000мс (1000 + 5000)
D) 11000мс (1000 + 5000 + 5000)

**Відповідь:** C —
Attempt 1 (t=0): відмова → delay 1000мс.
Attempt 2 (t=1000): відмова → delay 1000 * 5 = 5000мс.
Attempt 3 (t=6000): третя спроба.

---

**Питання 2:**
Яка головна перевага non-blocking retry (через Kafka топіки) перед blocking retry (Spring Retry)?

A) Non-blocking retry швидший
B) Поки повідомлення чекає в retry-топіку, основний топік продовжує читатися
C) Non-blocking retry не вимагає підключення до Kafka
D) Non-blocking retry підтримує більше спроб

**Відповідь:** B —
Non-blocking: невдале повідомлення переміщується до retry-topic.
Основний топік продовжує читатися → throughput не падає під час retry очікування.

---

**Питання 3:**
Consumer отримав повідомлення, спробував 3 рази (всі відмовили), повідомлення потрапило у DLT.
`@DltHandler` закинув виключення. Що відбудеться при `dltStrategy=FAIL_ON_ERROR`?

A) Виключення замовчується — повідомлення вважається обробленим
B) Повідомлення відправляється до нового retry топіку
C) Виключення логується і Spring Kafka застосовує стандартний error handler
D) Consumer зупиняється і потребує ручного перезапуску

**Відповідь:** C —
`FAIL_ON_ERROR`: виключення у @DltHandler не замовчується.
Spring Kafka логує його і застосовує ErrorHandler (за замовчуванням — логування та продовження).
Повідомлення НЕ відправляється на повторну обробку.

---

**Питання 4:**
Яку інформацію містить Kafka header `kafka_backoff_next_elapse`?

A) Час від початку першої спроби до поточного моменту
B) Timestamp (мілісекунди) коли consumer повинен обробити повідомлення
C) Затримка між спробами у мілісекундах
D) Максимальна кількість спроб

**Відповідь:** B —
`kafka_backoff_next_elapse` — абсолютний timestamp (epoch ms) коли retry consumer
повинен спробувати обробити повідомлення.
Consumer читає header і чекає до цього моменту.

---

**Питання 5:**
Скільки Kafka топіків автоматично створює `@RetryableTopic(attempts="3")`?

A) 1 (тільки DLT)
B) 2 (retry-0 + DLT)
C) 3 (retry-0 + retry-1 + DLT)
D) 4 (retry-0 + retry-1 + retry-2 + DLT)

**Відповідь:** B —
attempts=3 → 1 основна + 2 retry спроби.
Retry топіків = attempts - 1 = 2. Але один з них — DLT.
Тобто: retry-0 (attempt 2) + DLT (attempt 3). Всього 2 додаткових топіки.

Корекція: `attempts=3` → 1 attempt у main topic + 1 в retry-0 + 1 в retry-1 (DLT).
Топіки: retry-0, retry-1/DLT або retry-0 + DLT = 2 нових топіки (+ main = 3 загалом).

---

**Питання 6:**
Consumer обробляє `09.orders.created-retry-0`. Спроба відмовила.
Де опиниться повідомлення далі?

A) Назад у `09.orders.created`
B) У `09.orders.created-retry-1`
C) У `09.orders.created-dlt` (одразу після retry-0)
D) Consumer буде повторювати на `retry-0` indefinitely

**Відповідь:** B —
Retry топіки послідовні: main → retry-0 → retry-1 → DLT.
Відмова на retry-0 → повідомлення переміщується до retry-1.

---

**Питання 7:**
Яка різниця між `@RetryableTopic` і `DefaultErrorHandler` з `BackOff`?

A) DefaultErrorHandler є non-blocking; @RetryableTopic є blocking
B) @RetryableTopic є non-blocking (через Kafka топіки); DefaultErrorHandler є blocking (in-memory)
C) Обидва однакові — просто різні API
D) DefaultErrorHandler підтримує DLT; @RetryableTopic — ні

**Відповідь:** B —
DefaultErrorHandler: retry в пам'яті, consumer блокується між спробами, стан втрачається при рестарті.
@RetryableTopic: retry через окремі Kafka топіки, non-blocking, стан в Kafka.

---

**Питання 8:**
Яке значення `stats.dlt` після: simulate-failure/1, send order, send order, send order?

A) 0 — тільки 1 відмова, повідомлення обробиться на retry-0
B) 1 — перше замовлення пройде DLT
C) 3 — кожне замовлення матиме 1 відмову
D) Неможливо визначити

**Відповідь:** A —
simulate-failure/1 планує лише 1 відмову.
Перше замовлення: attempt 1 → fail, attempt 2 (retry-0) → success.
Наступні два замовлення: без відмов → success на main topic.
dlt=0.

---

**Питання 9:**
Consumer отримав повідомлення у DLT. `@DltHandler` записав у базу і повернув успішно.
Чи може адмін пізніше "відтворити" це повідомлення?

A) Ні — DLT повідомлення видаляються після обробки
B) Так — DLT є звичайним Kafka топіком, можна читати через consumer або CLI
C) Тільки якщо retention ще не закінчився і тільки через Admin API
D) Ні — @DltHandler автоматично видаляє повідомлення

**Відповідь:** B —
DLT — звичайний Kafka топік з retention.
Адмін може: читати через `kafka-console-consumer --from-beginning`,
відправити у новий топік для повторної обробки, або запустити окремий consumer.

---

**Питання 10:**
`@RetryableTopic(attempts="1")`. Що відбудеться при відмові?

A) Одна retry спроба, потім DLT
B) Повідомлення одразу потрапить у DLT (без retry)
C) Помилка конфігурації — attempts мінімум 2
D) Consumer буде повторювати indefinitely

**Відповідь:** B —
attempts=1 означає 1 спроба всього (основна).
При відмові → одразу DLT (0 retry топіків).
Spring Kafka дозволяє attempts=1.