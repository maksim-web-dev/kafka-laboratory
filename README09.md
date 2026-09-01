# Branch 09 — Error Handling & Dead Letter Topic

## Що змінилося порівняно з branch08

| Аспект | branch08 | branch09 |
|--------|----------|----------|
| Consumer | notification-service | **payment-service** |
| Retry механізм | — | **`@RetryableTopic` (non-blocking)** |
| Retry топіки | — | **`09.orders.created-retry-0`, `-retry-1`** |
| Dead Letter Topic | — | **`09.orders.created-dlt`** |
| Backoff | — | **1s → 5s → DLT** |
| DLT handler | — | **`@DltHandler`** |
| Failure simulation | `simulate-failure/{count}` | **`POST /api/payments/simulate-failure/{count}`** |
| Порти | 9106/8112/8113/8114/8115 | **9108/8116/8117/8118** |

---

## Ключові концепції

### Проблема без DLT

```
Consumer читає повідомлення → обробка падає з RuntimeException
Spring Kafka: retry indefinitely → consumer застрягає на одному офсеті
Або: log & skip → повідомлення ВТРАЧЕНО без сліду
```

### Non-blocking Retry через Retry Topics

```
@RetryableTopic(attempts = "3", backoff = @Backoff(delay = 1000, multiplier = 5.0))

Attempt 1 (t=0ms):   09.orders.created          → RuntimeException ✗
Attempt 2 (t=1000ms): 09.orders.created-retry-0  → RuntimeException ✗
Attempt 3 (t=6000ms): 09.orders.created-retry-1  → RuntimeException ✗
                       09.orders.created-dlt       → @DltHandler ← "dead letter"
```

**Non-blocking** = інші повідомлення в `09.orders.created` продовжують оброблятись поки це повідомлення "чекає" в retry-топіку.

### Порівняння Blocking vs Non-blocking Retry

| | Blocking (Spring Retry) | Non-blocking (Retry Topics) |
|--|--|--|
| Реалізація | `@RetryableTopic` + AOP | `@RetryableTopic` + Kafka topics |
| Throughput під час retry | **зупиняється** (block poll) | **продовжується** (інші msg читаються) |
| Retry state | в пам'яті (втрачається при рестарті) | **в Kafka** (персистентно) |
| Visibility | прихований | **видимий у Kafka UI** |
| Delay точність | точний | приблизний (залежить від poll interval) |

### @RetryableTopic конфігурація

```kotlin
@RetryableTopic(
    attempts = "3",               // 1 initial + 2 retry = 3 total
    backoff = Backoff(
        delay = 1000,             // перший retry через 1с
        multiplier = 5.0          // другий retry через 5с
    ),
    topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
    dltStrategy = DltStrategy.FAIL_ON_ERROR
)
```

Автоматично створює:
```
09.orders.created          ← вже існує
09.orders.created-retry-0  ← auto-created, затримка 1с
09.orders.created-retry-1  ← auto-created, затримка 5с
09.orders.created-dlt      ← auto-created, кінцева точка відмови
```

### DLT Handler

```kotlin
@DltHandler
fun handleDlt(record: ConsumerRecord<String, OrderCreatedEvent>) {
    // Повідомлення не буде повторено
    // Тут: логування, alert у Slack/PagerDuty, запис у БД для manual review
    log.error("[DLT] Order {} requires manual intervention", record.value().orderId)
}
```

### Retry Headers

Кожне повідомлення в retry-топіку несе headers:
```
kafka_original_topic:     09.orders.created
kafka_original_partition: 0
kafka_original_offset:    42
kafka_exception-message:  Simulated payment failure for order abc-123
kafka_backoff_next_elapse: 1725000000000  ← timestamp коли обробляти
```

---

## Архітектура branch09

```
Order Service  →  09.orders.created (3 partitions)  →  payment-service-group
                                                          │
                                         ┌────────────────┴──────────────────┐
                                         │ @RetryableTopic(attempts=3)       │
                                         │                                   │
                                         │  fail → retry-0 (1s)             │
                                         │          retry-1 (5s)            │
                                         │          dlt → @DltHandler       │
                                         └───────────────────────────────────┘
```

---

## application.yml — зміни

```yaml
# payment-service
spring:
  kafka:
    consumer:
      group-id: payment-service-group
      auto-offset-reset: earliest
```

`@RetryableTopic` не потребує додаткових YAML налаштувань — всі параметри в анотації.

---

## Як запустити

```bash
# Зупинити branch08
docker stop kafka kafka-ui order-service-b08 \
  notification-service-1-b08 notification-service-2-b08 2>/dev/null || true

# Запустити branch09
docker compose -f docker-compose-09.yml down -v 2>/dev/null || true
docker compose -f docker-compose-09.yml up --build
```

Перевірити 4 контейнери:

```bash
docker compose -f docker-compose-09.yml ps
# kafka, kafka-ui, order-service-b09, payment-service-b09
```

---

## Як протестувати

### 1. Базова перевірка — успішна обробка

```bash
# Надіслати замовлення
curl -s -X POST http://localhost:8117/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-01","product":"Book","quantity":1,"totalAmount":25.00}' | jq

# Статистика payment-service
curl -s http://localhost:8118/api/payments/stats | jq
# {"processed":1,"dlt":0,"failNextN":0}
```

### 2. Головний демо-сценарій: retry → DLT

```bash
# Крок 1: Запланувати 3 послідовних відмови (одне повідомлення пройде всі retries)
curl -s -X POST http://localhost:8118/api/payments/simulate-failure/3 | jq
# {"scheduledFailures":3,"message":"Next 3 invocations will throw PaymentException → DLT"}

# Крок 2: Надіслати замовлення
curl -s -X POST http://localhost:8117/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-fail","product":"Doomed Order","quantity":1,"totalAmount":99.00}' | jq

# Логи payment-service покажуть:
# [RETRY] Attempt on topic=09.orders.created, orderId=abc-123
# (через 1с)
# [RETRY] Attempt on topic=09.orders.created-retry-0, orderId=abc-123
# (через 5с)
# [RETRY] Attempt on topic=09.orders.created-retry-1, orderId=abc-123
# [DLT] ⚠ Order abc-123 — topic=09.orders.created-dlt → Manual intervention required!

# Крок 3: Перевірити статистику
curl -s http://localhost:8118/api/payments/stats | jq
# {"processed":0,"dlt":1,"failNextN":0}
```

### 3. Partial retry — успіх після 1 retry

```bash
# Запланувати 1 відмову → повідомлення обробиться на retry-0
curl -s -X POST http://localhost:8118/api/payments/simulate-failure/1 | jq

curl -s -X POST http://localhost:8117/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-lucky","product":"Lucky Order","quantity":1,"totalAmount":50.00}' | jq

# Логи:
# [RETRY] Attempt on topic=09.orders.created → fail
# (через 1с)
# ╔═══ PAYMENT PROCESSED #1 (topic=09.orders.created-retry-0) ════╗
```

### 4. Перевірити retry-топіки у Kafka UI

Відкрити: [http://localhost:8116](http://localhost:8116)

- `Topics` → бачимо 4 топіки:
  - `09.orders.created`
  - `09.orders.created-retry-0`
  - `09.orders.created-retry-1`
  - `09.orders.created-dlt`
- `09.orders.created-dlt` → Messages → бачимо повідомлення з retry headers

### 5. Перевірити retry headers через CLI

```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic 09.orders.created-dlt \
  --from-beginning \
  --property print.headers=true \
  --max-messages 1
```

---

## Параметри @RetryableTopic

| Параметр | Branch09 | Пояснення |
|----------|----------|-----------|
| `attempts` | `"3"` | 1 основна + 2 retry спроби |
| `backoff.delay` | `1000` | перший retry через 1000мс |
| `backoff.multiplier` | `5.0` | delay * 5 для кожного наступного retry |
| `dltStrategy` | `FAIL_ON_ERROR` | DLT handler не повторює, але може кидати exception |
| `topicSuffixingStrategy` | `SUFFIX_WITH_INDEX_VALUE` | суфікс `-retry-0`, `-retry-1` |
| `autoCreateTopics` | `true` (default) | авто-створення retry і DLT топіків |

---

## Структура змін відносно branch08

```
kafka-laboratory/
├── docker-compose-09.yml                              ← нові порти b09 (9108/8116/8117/8118)
├── branch09_error_retry/
│   ├── order-service/                                 ← топік 09.orders.created
│   └── payment-service/                               ← NEW сервіс
│       └── src/main/kotlin/.../listener/
│           └── OrderPaymentListener.kt                ← @RetryableTopic, @DltHandler
│       └── src/main/kotlin/.../controller/
│           └── PaymentController.kt                   ← /stats, /simulate-failure
│       └── src/main/kotlin/.../exception/
│           └── PaymentException.kt                    ← RuntimeException subclass
└── README09.md
```

---

## Ключові концепції цієї гілки

| Концепція | Що демонструє |
|-----------|---------------|
| **`@RetryableTopic`** | Non-blocking retry через окремі Kafka топіки |
| **Retry Topics** | `retry-0`, `retry-1` — персистентна черга повторних спроб |
| **Dead Letter Topic** | Кінцева точка для повідомлень що не вдалося обробити |
| **`@DltHandler`** | Обробник DLT: логування, alerting, manual review |
| **Backoff strategy** | Exponential backoff: 1s → 5s (delay * multiplier) |
| **Non-blocking** | Retry повідомлення не блокує обробку нових повідомлень |
| **Retry Headers** | Kafka headers з інформацією про причину і оригінальний топік |

---

## Що далі — branch10

У наступній гілці вивчаємо **JSON Serialization**:
- Typed event-класи з Jackson
- `JsonSerializer` / `JsonDeserializer` в Spring Kafka
- Event versioning: поле `eventVersion`
- TYPE_MAPPINGS для безпечної десеріалізації