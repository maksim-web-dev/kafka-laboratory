# Branch 12 — Idempotent Producer & Transactions

## Що вивчаємо

| Концепція | Деталі |
|-----------|--------|
| **Idempotent Producer** | `enable.idempotence=true`, sequence numbers, дедублікація повторів |
| **Kafka Transactions** | `transactional.id`, `beginTransaction` / `commitTransaction` / `abortTransaction` |
| **Exactly-Once Semantics (EOS)** | Повідомлення або committed, або відкинуто — без дублікатів і втрат |
| **`read_committed`** | Consumer бачить лише committed повідомлення, ігнорує abort |
| **`executeInTransaction`** | Spring Kafka API для транзакційного відправлення |

## Архітектура

```
OrderService ──[12.orders.created]──► PaymentService ──[12.payments.processed]──► NotificationService
  (idempotent producer)                (transactional producer)                     (read_committed)
  enable.idempotence=true              transaction-id-prefix: payment-tx-           isolation-level: read-committed
  acks=all, retries=MAX
```

### Нормальний flow (commit)
```
1. OrderService    → publish OrderCreatedEvent (idempotent)
2. PaymentService  → beginTransaction
3. PaymentService  → publish PaymentProcessedEvent
4. PaymentService  → commitTransaction  ✅
5. NotificationService → бачить PaymentProcessedEvent (read_committed)
```

### Abort flow
```
1. OrderService    → publish OrderCreatedEvent (idempotent)
2. PaymentService  → beginTransaction
3. PaymentService  → publish PaymentProcessedEvent
4. PaymentService  → abortTransaction (RuntimeException)  ❌
5. NotificationService → НЕ бачить повідомлення (read_committed ігнорує abort)
```

## Idempotent Producer

Проблема без idempotence: якщо broker не встиг надіслати ACK, producer повторює відправлення → **дублікат**.

З `enable.idempotence=true` кожне повідомлення отримує `producer ID` + `sequence number`. Broker відкидає дублікати автоматично.

**Вимоги:**
- `acks=all` — обов'язково
- `retries` > 0 — для повторних спроб
- `max.in.flight.requests.per.connection` ≤ 5

```yaml
# order-service/application.yml
spring:
  kafka:
    producer:
      acks: all
      retries: 2147483647
      properties:
        enable.idempotence: true
        max.in.flight.requests.per.connection: 5
```

## Transactional Producer

`transactional.id` — унікальний ідентифікатор продюсера. Kafka використовує його для відновлення незавершених транзакцій після рестарту.

Spring Kafka автоматично створює транзакційний `ProducerFactory` та `KafkaTransactionManager` при наявності `transaction-id-prefix`.

```yaml
# payment-service/application.yml
spring:
  kafka:
    producer:
      transaction-id-prefix: payment-tx-
```

```kotlin
// OrderPaymentListener.kt
kafkaTemplate.executeInTransaction { kt ->
    kt.send("12.payments.processed", orderId, payment)
    // Якщо тут кидаємо виняток → транзакція відкочується
}
```

## Consumer з `read_committed`

| Режим | Поведінка |
|-------|-----------|
| `read_uncommitted` (default) | Бачить всі повідомлення, включно з тими що ще в незакритій транзакції |
| `read_committed` | Бачить лише committed повідомлення; abort і незакриті транзакції — ігноруються |

```yaml
# notification-service/application.yml
spring:
  kafka:
    consumer:
      isolation-level: read-committed
```

## Порти

| Сервіс | Порт |
|--------|------|
| Kafka (external) | 9113 |
| Schema Registry | 8130 |
| Kafka UI | 8129 |
| order-service | 8131 |
| payment-service | 8132 |
| notification-service | 8133 |

## Запуск

```bash
docker compose -f docker-compose-12.yml up --build
```

Kafka UI: http://localhost:8129

## API

### Створити замовлення (idempotent producer)
```bash
curl -s -X POST http://localhost:8131/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-42", "itemCount": 2}' | jq .
```

### Симулювати abort транзакції

Позначає наступні N платежів для abort. Notification Service не отримає жодного повідомлення.

```bash
curl -s -X POST "http://localhost:8132/api/payments/abort-next?count=1"
```

Потім створюємо замовлення і в логах notification-service переконуємось, що повідомлення **не з'явилось**.

## Демо сценарій

```bash
# 1. Нормальний flow — notification-service отримає PaymentProcessedEvent
curl -s -X POST http://localhost:8131/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-1"}' | jq .

# 2. Позначаємо наступну транзакцію для abort
curl -s -X POST "http://localhost:8132/api/payments/abort-next?count=1"

# 3. Створюємо ще одне замовлення — транзакція відкотиться
curl -s -X POST http://localhost:8131/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-2"}' | jq .

# В логах notification-service: перше замовлення є, друге — відсутнє
# В Kafka UI: у topic 12.payments.processed буде ABORT marker
```

## Що видно в Kafka UI

- Топік `12.payments.processed` → повідомлення з committed транзакцій видно як звичайні
- Повідомлення з aborted транзакцій помічені як `control batch` з типом `ABORT` — вони фізично є в log-файлі, але `read_committed` consumer їх пропускає

## Стек

- Spring Boot 3.3.4 + Spring Kafka
- Apache Avro 1.11.4
- Confluent `kafka-avro-serializer` 7.7.1
- Gradle Avro Plugin `com.github.davidmc24.gradle.plugin.avro` 1.9.1
- Confluent Schema Registry 8.0.1
- Confluent Kafka 8.0.1 (KRaft mode)