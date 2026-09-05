# Branch 13 — Saga Pattern (Choreography)

## Що вивчаємо

| Концепція | Деталі |
|-----------|--------|
| **Choreography Saga** | Сервіси реагують на події одне одного без центрального оркестра |
| **Compensating Transactions** | При збої — відкатуємо попередні кроки через компенсуючі події |
| **Idempotency Key** | Поле в події для захисту від дублювання при retry |
| **Розподілені транзакції** | Saga замість 2PC — без блокувань, без SPOF |

## Архітектура Saga (Choreography)

```
┌─────────────────────────────────────────────────────────────────┐
│                    HAPPY PATH (5 кроків)                        │
│                                                                 │
│  [1] OrderService  ──[13.orders.created]──►  PaymentService     │
│  [2] PaymentService ──[13.payments.processed]──► InventoryService│
│  [3] InventoryService ──[13.inventory.reserved]──► OrderService  │
│  [4] OrderService  ──[13.orders.confirmed]──► NotificationService│
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│              COMPENSATION (inventory fail)                      │
│                                                                 │
│  InventoryService ──[13.inventory.failed]──► PaymentService (refund)
│                                         └──► OrderService (cancel)
│  PaymentService   ──[13.payments.refunded]──► NotificationService│
│  OrderService     ──[13.orders.cancelled]──► NotificationService│
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│              COMPENSATION (payment fail)                        │
│                                                                 │
│  PaymentService ──[13.payments.failed]──► OrderService (cancel) │
│  OrderService   ──[13.orders.cancelled]──► NotificationService  │
└─────────────────────────────────────────────────────────────────┘
```

## Топіки

| Топік | Продюсер | Консюмери |
|-------|----------|-----------|
| `13.orders.created` | order-service | payment-service, notification-service |
| `13.payments.processed` | payment-service | inventory-service, notification-service |
| `13.payments.failed` | payment-service | order-service, notification-service |
| `13.payments.refunded` | payment-service | notification-service |
| `13.inventory.reserved` | inventory-service | order-service, notification-service |
| `13.inventory.failed` | inventory-service | payment-service, order-service, notification-service |
| `13.orders.confirmed` | order-service | notification-service |
| `13.orders.cancelled` | order-service | notification-service |

## Idempotency Key

Кожне `OrderCreatedEvent` містить `idempotencyKey = "order-created:{orderId}"`.  
Payment Service перевіряє цей ключ і **відкидає дублікати** — важливо при retry після збою.

```kotlin
if (!processedKeys.add(e.idempotencyKey.toString())) {
    log.warn("Duplicate idempotencyKey — skipped")
    return
}
```

## Порти

| Сервіс | Порт |
|--------|------|
| Kafka (external) | 9114 |
| Schema Registry | 8135 |
| Kafka UI | 8134 |
| order-service | 8136 |
| payment-service | 8137 |
| inventory-service | 8138 |
| notification-service | 8139 |

## Запуск

```bash
docker compose -f docker-compose-13.yml up --build
```

Kafka UI: http://localhost:8134

## API

### Створити замовлення (happy path)
```bash
curl -s -X POST http://localhost:8136/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-42", "itemCount": 2}' | jq .
```

### Перевірити статус замовлення
```bash
curl -s http://localhost:8136/api/orders/{orderId}/status | jq .
```

### Симулювати збій оплати → order cancelled
```bash
curl -s -X POST "http://localhost:8137/api/payments/fail-next?count=1"
# Потім створити замовлення — saga compensation: order cancelled
```

### Симулювати відсутність товару → payment refunded + order cancelled
```bash
curl -s -X POST "http://localhost:8138/api/inventory/out-of-stock?count=1"
# Потім створити замовлення — saga compensation: refund + cancel
```

## Демо сценарій

```bash
# 1. Happy path — в логах notification-service: STEP 1 → 2 → 3 → CONFIRMED
curl -s -X POST http://localhost:8136/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-1"}' | jq .

# 2. Збій платежу → ORDER CANCELLED
curl -s -X POST "http://localhost:8137/api/payments/fail-next?count=1"
curl -s -X POST http://localhost:8136/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-2"}' | jq .

# 3. Збій складу → PAYMENT REFUNDED + ORDER CANCELLED
curl -s -X POST "http://localhost:8138/api/inventory/out-of-stock?count=1"
curl -s -X POST http://localhost:8136/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-3"}' | jq .
```

## Choreography vs Orchestration

| | Choreography (branch13) | Orchestration |
|--|--|--|
| **Координація** | Сервіси самі реагують на події | Центральний оркестр дає команди |
| **Зв'язність** | Слабка (знають лише про події) | Сильна (оркестр знає про всіх) |
| **Простота** | Проста для малої кількості сервісів | Зрозуміліша для складних flow |
| **Дебагінг** | Важче відстежити повний flow | Легко — весь стан в оркестрі |
| **SPOF** | Немає | Оркестр — потенційний SPOF |

## Нові Avro-схеми

| Схема | Призначення |
|-------|-------------|
| `PaymentFailedEvent` | Збій оплати → trigger для скасування замовлення |
| `PaymentRefundedEvent` | Повернення коштів після збою складу |
| `InventoryReservedEvent` | Успішне резервування → trigger для підтвердження замовлення |
| `InventoryFailedEvent` | Збій складу → trigger для компенсації |
| `OrderConfirmedEvent` | Saga завершена успішно |
| `OrderCancelledEvent` | Saga завершена через компенсацію |

## Стек

- Spring Boot 3.3.4 + Spring Kafka
- Apache Avro 1.11.4
- Confluent `kafka-avro-serializer` 7.7.1
- Gradle Avro Plugin `com.github.davidmc24.gradle.plugin.avro` 1.9.1
- Confluent Schema Registry 8.0.1
- Confluent Kafka 8.0.1 (KRaft mode)