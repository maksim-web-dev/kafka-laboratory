# Branch 10 — JSON Serialization

## Що змінилося порівняно з branch09

| Аспект | branch09 | branch10 |
|--------|----------|----------|
| Сервіси | order + payment | **order + payment + notification** |
| Event model | плоскі поля | **`eventVersion` + `items: List<OrderItem>`** |
| Тип mapping | — | **`spring.json.type.mapping` (alias → FQCN)** |
| Type headers | — | **`spring.json.add.type.headers: true`** |
| Pipeline | order → payment | **order → payment → notification** |
| Новий топік | — | **`10.payments.processed`** |
| `PaymentProcessedEvent` | — | **NEW: `APPROVED` / `DECLINED`** |
| Порти | 9108/8116/8117/8118 | **9110/8120/8121/8122/8123** |

---

## Що вивчаємо

- `JsonSerializer` / `JsonDeserializer` — Spring Kafka вбудована серіалізація
- `spring.json.add.type.headers: true` — type header у кожному повідомленні
- `spring.json.type.mapping` — маппінг alias → FQCN (розв'язує cross-service проблему)
- `spring.json.trusted.packages` — whitelist пакетів для десеріалізатора
- Event versioning — поле `eventVersion` для backward compatibility
- Nested objects — `OrderItem` всередині `OrderCreatedEvent`
- Pipeline: `10.orders.created` → payment-service → `10.payments.processed` → notification-service

---

## Архітектура branch10

```
order-service          payment-service              notification-service
─────────────         ───────────────              ────────────────────
createOrder()    →    handleOrderCreated()    →    handlePaymentProcessed()
OrderCreatedEvent  →  PaymentProcessedEvent   →    logs APPROVED/DECLINED
  eventVersion=1.0      status: APPROVED|DECLINED
  items: [OrderItem]    paymentId
                        orderId

                   DLT → @DltHandler
                        PaymentProcessedEvent(DECLINED)
```

### Топіки

| Топік                       | Партиції | Producer        | Consumer(s)                      |
|-----------------------------|----------|-----------------|----------------------------------|
| `10.orders.created`         | 3        | order-service   | payment-service                  |
| `10.orders.cancelled`       | 1        | order-service   | notification-service             |
| `10.payments.processed`     | 3        | payment-service | notification-service             |
| `10.orders.created-retry-0` | 3        | Spring Kafka    | payment-service (auto)           |
| `10.orders.created-retry-1` | 3        | Spring Kafka    | payment-service (auto)           |
| `10.orders.created-dlt`     | 3        | Spring Kafka    | payment-service (@DltHandler)    |

---

## Ключові концепції

### Type header проблема

Без маппінгу Kafka серіалізатор записує FQCN у заголовок `__TypeId__`:
```
__TypeId__: com.kafkalab.order.model.OrderCreatedEvent
```

Десеріалізатор в іншому сервісі отримує цей клас — якого там немає → `ClassNotFoundException`.

### Рішення: `spring.json.type.mapping`

```yaml
# Producer (order-service):
spring.json.type.mapping: "OrderCreatedEvent:com.kafkalab.order.model.OrderCreatedEvent"
# Заголовок: __TypeId__: OrderCreatedEvent  ← alias, не FQCN

# Consumer (payment-service):
spring.json.type.mapping: "OrderCreatedEvent:com.kafkalab.payment.model.OrderCreatedEvent"
# Alias "OrderCreatedEvent" → локальний клас payment-service
```

### Event versioning

```kotlin
data class OrderCreatedEvent(
    val eventVersion: String = "1.0",       // backward compatibility
    val orderId: String = UUID.randomUUID().toString(),
    val items: List<OrderItem> = emptyList(),
    ...
)
```

Якщо в майбутньому з'явиться `eventVersion = "2.0"`, consumer може відрізнити версії та обробляти відповідно.

### Nested objects

`OrderItem` серіалізується як JSON array — окремий `type.mapping` не потрібен:
```json
{
  "eventVersion": "1.0",
  "orderId": "uuid",
  "items": [
    { "productId": "uuid", "productName": "Laptop", "quantity": 1, "unitPrice": 1500.0 }
  ],
  "totalAmount": 1500.0
}
```

### DLT → DECLINED flow

```
payment-service: @DltHandler отримує повідомлення після 3 невдалих спроб
  → публікує PaymentProcessedEvent(status=DECLINED)
  → notification-service отримує DECLINED подію і логує
```

---

## application.yml — зміни

```yaml
# order-service (producer):
spring.json.add.type.headers: true
spring.json.type.mapping: "OrderCreatedEvent:com.kafkalab.order.model.OrderCreatedEvent,OrderCancelledEvent:com.kafkalab.order.model.OrderCancelledEvent"

# payment-service (consumer):
spring.json.type.mapping: "OrderCreatedEvent:com.kafkalab.payment.model.OrderCreatedEvent"

# payment-service (producer):
spring.json.add.type.headers: true
spring.json.type.mapping: "PaymentProcessedEvent:com.kafkalab.payment.model.PaymentProcessedEvent"

# notification-service (consumer):
spring.json.type.mapping: "OrderCreatedEvent:com.kafkalab.notification.model.OrderCreatedEvent,OrderCancelledEvent:com.kafkalab.notification.model.OrderCancelledEvent,PaymentProcessedEvent:com.kafkalab.notification.model.PaymentProcessedEvent"
```

---

## Як запустити

```bash
# Зупинити branch09
docker stop kafka kafka-ui order-service-b09 payment-service-b09 2>/dev/null || true

# Запустити branch10
docker compose -f docker-compose-10.yml down -v 2>/dev/null || true
docker compose -f docker-compose-10.yml up --build
```

Перевірити 5 контейнерів:

```bash
docker compose -f docker-compose-10.yml ps
# kafka, kafka-ui, order-service-b10, payment-service-b10, notification-service-b10
```

---

## Як протестувати

### 1. Створити замовлення

```bash
curl -s -X POST http://localhost:8121/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"alice","product":"Laptop","quantity":1,"totalAmount":1500.0}' | jq
```

Очікувана відповідь містить `eventVersion` та `items`:
```json
{
  "eventVersion": "1.0",
  "orderId": "uuid",
  "userId": "alice",
  "items": [{ "productName": "Laptop", "quantity": 1, "unitPrice": 1500.0, ... }],
  "totalAmount": 1500.0
}
```

### 2. Перевірити payment-service отримав подію

```bash
curl -s http://localhost:8122/api/payments/stats | jq
# {"processed":1,"dlt":0,"failNextN":0}
```

### 3. Перевірити notification-service отримав обидві події

```bash
curl -s http://localhost:8123/api/notifications/count | jq
# {
#   "orders_created": 1,
#   "orders_cancelled": 0,
#   "payments_processed": 1,
#   "total": 2
# }
```

### 4. Симулювати збій платежу (DLT → DECLINED)

```bash
# Запланувати 3 збої → одне повідомлення пройде retry-0 → retry-1 → DLT → DECLINED
curl -s -X POST http://localhost:8122/api/payments/simulate-failure/3 | jq

curl -s -X POST http://localhost:8121/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"bob","product":"Headphones","quantity":1,"totalAmount":200.0}' | jq

# Перевірити: payments_processed++ зі статусом DECLINED у логах notification-service
curl -s http://localhost:8123/api/notifications/count | jq
```

### 5. Kafka UI — перегляд type headers

Відкрити: [http://localhost:8120](http://localhost:8120)

- `Topics` → `10.orders.created` → будь-яке повідомлення → вкладка **Headers**:
  - `__TypeId__: OrderCreatedEvent` ← alias, не FQCN
- `Topics` → `10.payments.processed` → Headers:
  - `__TypeId__: PaymentProcessedEvent`

### 6. Config endpoint

```bash
curl -s http://localhost:8123/api/notifications/config | jq
# {
#   "listeners": ["10.orders.created", "10.orders.cancelled", "10.payments.processed"],
#   "typeMappings": ["OrderCreatedEvent → ...", ...]
# }
```

---

## Структура змін відносно branch09

```
kafka-laboratory/
├── docker-compose-10.yml                              ← 5 сервісів, порти b10 (9110/8120/8121/8122/8123)
├── branch10_json_serialization/
│   ├── order-service/
│   │   └── model/
│   │       ├── OrderCreatedEvent.kt                  ← +eventVersion, +items: List<OrderItem>
│   │       └── OrderItem.kt                          ← NEW nested object
│   ├── payment-service/
│   │   ├── model/
│   │   │   ├── PaymentProcessedEvent.kt              ← NEW
│   │   │   └── PaymentStatus.kt                      ← NEW enum APPROVED/DECLINED
│   │   └── listener/OrderPaymentListener.kt          ← @DltHandler публікує DECLINED event
│   └── notification-service/                         ← NEW сервіс
│       └── listener/NotificationListener.kt          ← слухає 3 топіки
└── README10.md
```

---

## Ключові концепції цієї гілки

| Концепція | Що демонструє |
|-----------|---------------|
| **`spring.json.type.mapping`** | Alias замість FQCN у `__TypeId__` header |
| **`spring.json.trusted.packages`** | Whitelist для безпечної десеріалізації |
| **`spring.json.add.type.headers`** | Додає `__TypeId__` до кожного повідомлення |
| **Event versioning** | `eventVersion` для backward compatibility |
| **Nested objects** | `List<OrderItem>` серіалізується автоматично |
| **Multi-topic pipeline** | 3-сервісний ланцюг через 3 топіки |
| **DLT → business event** | @DltHandler публікує DECLINED замість тихого логування |

---

## Що далі — branch11

У наступній гілці вивчаємо **Schema Registry & Avro**:
- Confluent Schema Registry в Docker
- Apache Avro: `.avsc` схеми
- Avro serializer/deserializer
- Еволюція схем: backward/forward/full compatibility