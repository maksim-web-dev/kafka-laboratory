# Branch 13 — Saga Pattern (Choreography)

## 1. Що вивчаємо у цій гілці

- **Choreography Saga** — патерн розподілених транзакцій без центрального координатора: кожен сервіс
  реагує на події інших і сам вирішує, що публікувати далі.
- **Compensating Transactions** — при збої будь-якого кроку попередні кроки відкочуються через
  спеціальні компенсуючі події (refund, cancel).
- **Idempotency Key** — поле `idempotencyKey` в кожній події захищає від повторної обробки при retry:
  `processedKeys.add(key)` повертає `false` для дублікатів.
- **Розподілені транзакції без 2PC** — жодного блокування, жодного Single Point of Failure.
- **8 Kafka топіків** на один бізнес-процес "замовлення".
- **Avro + Schema Registry** — строга типізація всіх подій між 4 сервісами.

---

## 2. Зміни порівняно з попередньою гілкою (branch12)

- **Додано** inventory-service — новий четвертий мікросервіс.
- **Замість 2 топіків** (orders.created, payments.processed) тепер **8 топіків** для повного саги-флоу.
- **Нові Avro-схеми**: PaymentFailedEvent, PaymentRefundedEvent, InventoryReservedEvent,
  InventoryFailedEvent, OrderConfirmedEvent, OrderCancelledEvent.
- **Видалено** Kafka Transactions (`executeInTransaction`) — саги використовують інший підхід до
  узгодженості.
- **Додано** idempotency key перевірку в PaymentService.
- **Архітектура** змінилась з лінійного ланцюга на граф компенсації.

---

## 3. Архітектура

```
POST /api/orders
      │
      ▼
order-service
  ──[13.orders.created]──────────────────────► payment-service
                                                     │
                                        OK → [13.payments.processed]
                                       FAIL → [13.payments.failed] ──────────► order-service
                                                     │                           (cancel)
                                                     ▼
                                              inventory-service
                                                     │
                                        OK → [13.inventory.reserved] ──► order-service
                                                                           (confirm)
                                       FAIL → [13.inventory.failed] ──── payment-service
                                                                       └── order-service
                                                     │
                                              payment-service
                                              [13.payments.refunded]

notification-service слухає ВСІ 8 топіків і виводить поточний крок саги
```

**Топіки та їх учасники:**
- `13.orders.created` (3 партиції) — продюсер: order; консюмери: payment, notification
- `13.payments.processed` — продюсер: payment; консюмери: inventory, notification
- `13.payments.failed` — продюсер: payment; консюмери: order, notification
- `13.payments.refunded` — продюсер: payment; консюмер: notification
- `13.inventory.reserved` — продюсер: inventory; консюмери: order, notification
- `13.inventory.failed` — продюсер: inventory; консюмери: payment, order, notification
- `13.orders.confirmed` — продюсер: order; консюмер: notification
- `13.orders.cancelled` — продюсер: order; консюмер: notification

**Сервіси та порти (з docker-compose-13.yml):**
- `kafka` — 9092 (внутрішній, KRaft single-node)
- `schema-registry-b13` — 8090
- `kafka-ui` — 8080 → http://localhost:8080
- `order-service-b13` — 8081 → http://localhost:8081
- `payment-service-b13` — 8083 → http://localhost:8083
- `inventory-service-b13` — 8085 → http://localhost:8085
- `notification-service-b13` — 8082 → http://localhost:8082

---

## 4. Ключові концепції

### Choreography vs Orchestration

```
Choreography (branch13):
  Сервіси самі реагують на події — "я побачив подію X, тому публікую Y"
  Зв'язність: слабка (кожен сервіс знає лише свої вхідні/вихідні топіки)
  Дебагінг: складніший — flow розподілений між сервісами

Orchestration (альтернатива):
  Центральний оркестр дає команди — "зроби крок 1, тепер крок 2"
  Зв'язність: сильна (оркестр знає всіх учасників)
  Дебагінг: простіший — весь стан в одному місці
  Але: оркестр — потенційний SPOF
```

### Compensating Transactions

```
Happy path: order.created → payment.processed → inventory.reserved → order.confirmed

Compensation (inventory fail):
  inventory.failed → payment.refunded (компенсація payment)
                   → order.cancelled  (компенсація order)

Compensation (payment fail):
  payment.failed → order.cancelled (компенсація order)

Важливо: компенсуючі транзакції МАЮТЬ бути ідемпотентними
```

### Idempotency Key

```kotlin
// payment-service: захист від повторної обробки при retry
private val processedKeys = ConcurrentHashMap.newKeySet<String>()

fun handleOrderCreated(event: OrderCreatedEvent) {
    if (!processedKeys.add(event.idempotencyKey.toString())) {
        log.warn("Duplicate idempotencyKey — skipped: {}", event.idempotencyKey)
        return
    }
    // ... process payment ...
}
```

`idempotencyKey = "order-created:{orderId}"` — унікальний per замовлення. При retry той самий
ключ вже є в `processedKeys` → обробка пропускається.

### Saga State Machine

```
OrderService внутрішній стан:
  PENDING → (payment ok) → PAYMENT_APPROVED
          → (inventory ok) → CONFIRMED
          → (payment fail) → CANCELLED
          → (inventory fail) → CANCELLED

API: GET /api/orders/{orderId}/status повертає поточний стан
```

---

## 5. Як запустити

```bash
# Запустити всі сервіси
docker compose -f docker-compose-13.yml up --build

# Перший запуск займає ~3 хв (збірка 4 сервісів + Avro генерація)
# Kafka UI: http://localhost:8080
```

**Переконатися що все готово:**
```bash
# Всі 4 сервіси мають бути UP
docker compose -f docker-compose-13.yml ps

# Перевірити що 8 топіків створено (auto.create.topics=false!)
curl http://localhost:8080/api/clusters/branch13/topics | jq '.[].name'
```

---

## 6. Як тестувати

### Сценарій 1 — Happy Path

```bash
# Відправити замовлення
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-42", "itemCount": 2}' | jq .

# Лог notification-service (в порядку):
# [SAGA 1/5] ORDER CREATED     orderId=...
# [SAGA 2/5] PAYMENT APPROVED  orderId=...
# [SAGA 3/5] INVENTORY RESERVED orderId=...
# [SAGA 4/5] ORDER CONFIRMED   orderId=...
# [SAGA DONE ✅]

# Перевірити статус замовлення
curl -s http://localhost:8081/api/orders/{orderId}/status | jq .
# {"status": "CONFIRMED"}
```

### Сценарій 2 — Compensation: збій платежу

```bash
# Наступний платіж провалиться
curl -s -X POST "http://localhost:8083/api/payments/fail-next?count=1"

# Відправити замовлення
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-99"}' | jq .

# Лог notification-service:
# [SAGA 1/5] ORDER CREATED
# [SAGA COMP] PAYMENT FAILED → compensation triggered
# [SAGA DONE ❌] ORDER CANCELLED reason=Payment failed
```

### Сценарій 3 — Compensation: відсутній товар на складі

```bash
# Наступне резервування провалиться
curl -s -X POST "http://localhost:8085/api/inventory/out-of-stock?count=1"

# Відправити замовлення
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-77"}' | jq .

# Лог notification-service:
# [SAGA 2/5] PAYMENT APPROVED
# [SAGA COMP] INVENTORY FAILED → payment refunded + order cancelled
# [SAGA DONE ❌] ORDER CANCELLED reason=Out of stock
```

### Сценарій 4 — Перевірка idempotency key

```bash
# Відправити одне і теж замовлення двічі (симуляція retry)
BODY='{"userId": "user-1"}'
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" -d "$BODY" | jq .orderId

# Повторно надіслати те ж повідомлення через kafka-producer (або через API двічі)
# Лог payment-service: "Duplicate idempotencyKey — skipped"
```

---

## 7. Поглиблений розгляд

### Чому 8 топіків?

Кожен топік — це контракт між двома сервісами. 8 топіків = 8 бізнес-подій у процесі замовлення.
На відміну від REST (синхронний виклик), Kafka топіки дають:
- **Decoupling** — сервіси не знають адрес один одного
- **Durability** — повідомлення зберігаються навіть якщо сервіс не запущений
- **Replay** — можна перечитати з будь-якого offset

### Проблеми Choreography при масштабуванні

```
При 5+ сервісах граф залежностей стає складним:
  - Важко зрозуміти повний flow без схеми
  - Тестування потребує запуску всіх сервісів
  - Дедлайни/таймаути між кроками важко гарантувати

Рішення: додати Saga State Store (Redis/DB) у кожному сервісі
  або перейти на Orchestration (Temporal, Conductor)
```

### Avro схеми для компенсуючих подій

```json
// InventoryFailedEvent.avsc
{
  "type": "record",
  "name": "InventoryFailedEvent",
  "fields": [
    {"name": "orderId", "type": "string"},
    {"name": "paymentId", "type": "string"},
    {"name": "reason", "type": "string"},
    {"name": "occurredAt", "type": "long"}
  ]
}
```

`paymentId` передається в `InventoryFailedEvent`, щоб payment-service знав який платіж рефандити.
Це — приклад **event enrichment**: подія несе достатньо даних для компенсації без запиту до БД.

### Schema Registry для 4 сервісів

```yaml
# кожен сервіс в application.yml
spring:
  kafka:
    properties:
      schema.registry.url: ${SCHEMA_REGISTRY_URL:http://localhost:8090}
    producer:
      value-serializer: io.confluent.kafka.serializers.KafkaAvroSerializer
    consumer:
      value-deserializer: io.confluent.kafka.serializers.KafkaAvroDeserializer
      properties:
        specific.avro.reader: true
```

---

## 8. Структура проекту

```
branch13_saga_pattern/
├── order-service/
│   ├── src/main/kotlin/.../
│   │   ├── controller/OrderController.kt     ← POST /api/orders, GET /status
│   │   ├── service/OrderService.kt           ← Saga init, state machine
│   │   ├── listener/
│   │   │   ├── InventoryReservedListener.kt  ← publish orders.confirmed
│   │   │   ├── InventoryFailedListener.kt    ← publish orders.cancelled
│   │   │   └── PaymentFailedListener.kt      ← publish orders.cancelled
│   │   └── config/KafkaTopicConfig.kt        ← 8 TopicBuilder
│   └── src/main/avro/
│       ├── OrderCreatedEvent.avsc
│       ├── OrderConfirmedEvent.avsc
│       └── OrderCancelledEvent.avsc
├── payment-service/
│   ├── listener/OrderCreatedListener.kt      ← idempotency check, approve/fail
│   ├── listener/InventoryFailedListener.kt   ← refund payment
│   └── src/main/avro/
│       ├── PaymentProcessedEvent.avsc
│       ├── PaymentFailedEvent.avsc
│       └── PaymentRefundedEvent.avsc
├── inventory-service/
│   ├── listener/PaymentProcessedListener.kt  ← reserve/fail
│   └── src/main/avro/
│       ├── InventoryReservedEvent.avsc
│       └── InventoryFailedEvent.avsc
└── notification-service/
    └── listener/                             ← слухає всі 8 топіків, логує крок саги
```

---

## 9. Що далі

- **Branch 14** — Kafka Streams: замість слухання кожного топіку окремо — обробка потоку через
  KStream/KTable, агрегації, tumbling windows, state stores.
- **Branch 19** — Production-like demo: branch13 + SASL/ACL + cluster + monitoring.

---

## 10. Слайди для лекції

### Слайд 1 — Проблема розподілених транзакцій

```
Що, якщо між двома мікросервісами потрібна транзакція?

Варіант 1: 2PC (Two-Phase Commit)
  ✗ Блокувальний протокол
  ✗ Coordinator — SPOF
  ✗ Погано масштабується

Варіант 2: Saga Pattern
  ✓ Без блокування
  ✓ Немає SPOF
  ✓ Кожен сервіс виконує локальну транзакцію
  ! При збої — потрібні compensating transactions
```

### Слайд 2 — Choreography Saga Flow

```
Choreography = "без диригента"
Кожен сервіс знає: "якщо я бачу подію X — публікую Y"

order ──[created]──► payment ──[processed]──► inventory ──[reserved]──► order ──[confirmed]

При збої inventory:
  inventory ──[failed]──► payment ──[refunded]
                      └──► order ──[cancelled]
```

### Слайд 3 — Idempotency Key

```
Проблема: Kafka може доставити повідомлення двічі (at-least-once)
Якщо payment-service обробить двічі → подвійне списання!

Рішення: idempotencyKey = "order-created:{orderId}"
  Перший раз: processedKeys.add(key) → true  → обробляємо ✓
  Retry:      processedKeys.add(key) → false → пропускаємо ✓

В production: processedKeys зберігати в Redis/DB (не in-memory!)
```

---

## 11. Демонстраційний сценарій

```bash
#!/bin/bash
# Branch 13 — Saga Pattern Demo

echo "=== Запуск ==="
docker compose -f docker-compose-13.yml up --build -d
echo "Очікуємо 3 хвилини..."
sleep 180

echo ""
echo "=== Сценарій 1: Happy Path ==="
RESP=$(curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-1","itemCount":2}')
echo "Response: $RESP"
ORDER_ID=$(echo $RESP | jq -r '.orderId')
sleep 2
echo "Status: $(curl -s http://localhost:8081/api/orders/$ORDER_ID/status)"

echo ""
echo "=== Сценарій 2: Payment Failure ==="
curl -s -X POST "http://localhost:8083/api/payments/fail-next?count=1"
RESP=$(curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" -d '{"userId":"user-2"}')
echo "Response: $RESP"
ORDER_ID=$(echo $RESP | jq -r '.orderId')
sleep 2
echo "Status: $(curl -s http://localhost:8081/api/orders/$ORDER_ID/status)"

echo ""
echo "=== Сценарій 3: Inventory Failure ==="
curl -s -X POST "http://localhost:8085/api/inventory/out-of-stock?count=1"
RESP=$(curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" -d '{"userId":"user-3"}')
echo "Response: $RESP"
ORDER_ID=$(echo $RESP | jq -r '.orderId')
sleep 3
echo "Status: $(curl -s http://localhost:8081/api/orders/$ORDER_ID/status)"

echo ""
echo "=== Kafka UI: http://localhost:8080 ==="
echo "=== Переглянути 8 топіків та потік повідомлень ==="
```

---

## 12. Питання для самоперевірки

- Чим відрізняється Choreography Saga від Orchestration Saga?
- Навіщо потрібен idempotency key? Що буде без нього при retry?
- Якщо inventory.failed отримали payment-service та order-service одночасно — чи є race condition?
- Скільки Kafka повідомлень генерує один happy path? Один compensation path?
- Чому `AUTO_CREATE_TOPICS_ENABLE=false` є кращою практикою?
- Яка різниця між `compensating transaction` і звичайним rollback?
- Як Avro схема захищає від несумісних змін у InventoryFailedEvent?
- Що зберігає Schema Registry і яка його роль при десеріалізації?