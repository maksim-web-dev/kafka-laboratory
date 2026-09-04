# Branch 11 — Schema Registry & Avro

## Що вивчаємо

| Концепція | Деталі |
|-----------|--------|
| **Schema Registry** | Центральне сховище схем, версіонування, перевірка сумісності |
| **Apache Avro** | Бінарна серіалізація з `.avsc` схемами, генерація коду |
| **Schema Evolution** | Backward / Forward / Full compatibility |
| **Avro vs JSON** | Розмір повідомлення, строга типізація, відсутність назв полів у payload |

## Архітектура

```
OrderService   ──[11.orders.created]──►  PaymentService ──[11.payments.processed]──►
     │                                                                              │
     └──[11.orders.cancelled]──────────────────────────────────────────────────────►│
                                                                              NotificationService
                                        ▼
                               Schema Registry
                          (зберігає всі Avro схеми)
```

Кожен producer реєструє схему в Schema Registry при першій публікації.
Consumer отримує схему за schema ID, що вбудований у кожне повідомлення.

## Чому Avro + Schema Registry?

**JSON (branch10):**
- Кожне повідомлення містить назви полів → великий розмір
- Немає контракту → producer може надіслати будь-що
- `ClassNotFoundException` при зміні namespace

**Avro (branch11):**
- Назви полів зберігаються лише в схемі, payload — чисті байти → менший розмір
- Schema Registry відхиляє несумісну схему ще до публікації
- Еволюція схем без downtime (backward compatible зміни)

## Avro схеми (`.avsc`)

Кожен сервіс має власну копію схем у `src/main/avro/`.
Gradle Avro Plugin (`com.github.davidmc24.gradle.plugin.avro`) генерує Java-класи при збірці.

### OrderCreatedEvent
```json
{
  "type": "record",
  "name": "OrderCreatedEvent",
  "namespace": "com.kafkalab.avro",
  "fields": [
    {"name": "eventVersion", "type": "string", "default": "1.0"},
    {"name": "orderId",      "type": "string", "default": ""},
    {"name": "userId",       "type": "string", "default": ""},
    {"name": "items",        "type": {"type": "array", "items": "com.kafkalab.avro.OrderItem"}, "default": []},
    {"name": "totalAmount",  "type": "double", "default": 0.0},
    {"name": "timestamp",    "type": "string", "default": ""}
  ]
}
```

### PaymentProcessedEvent
```json
{
  "type": "record",
  "name": "PaymentProcessedEvent",
  "namespace": "com.kafkalab.avro",
  "fields": [
    {"name": "eventVersion", "type": "string", "default": "1.0"},
    {"name": "paymentId",    "type": "string", "default": ""},
    {"name": "orderId",      "type": "string", "default": ""},
    {"name": "userId",       "type": "string", "default": ""},
    {"name": "amount",       "type": "double", "default": 0.0},
    {"name": "status",       "type": "string", "default": ""},
    {"name": "timestamp",    "type": "string", "default": ""}
  ]
}
```

## Порти

| Сервіс | Порт |
|--------|------|
| Kafka (external) | 9112 |
| Schema Registry | 8125 |
| Kafka UI | 8124 |
| order-service | 8126 |
| payment-service | 8127 |
| notification-service | 8128 |

## Запуск

```bash
docker compose -f docker-compose-11.yml up --build
```

Kafka UI з підтримкою Schema Registry: http://localhost:8124

## API

### Створити замовлення
```bash
curl -s -X POST http://localhost:8126/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-42", "itemCount": 3}' | jq .
```

### Скасувати замовлення
```bash
curl -s -X POST http://localhost:8126/api/orders/{orderId}/cancel \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-42", "reason": "Changed mind"}'
```

### Симулювати збій платежу
```bash
curl -s -X POST "http://localhost:8127/api/payments/fail-next?count=1"
```

## Schema Registry REST API

### Переглянути всі зареєстровані схеми
```bash
curl -s http://localhost:8125/subjects | jq .
```

### Переглянути конкретну схему
```bash
curl -s http://localhost:8125/subjects/11.orders.created-value/versions/latest | jq .
```

### Перевірити сумісність нової схеми
```bash
curl -s -X POST http://localhost:8125/compatibility/subjects/11.orders.created-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schema": "{\"type\":\"record\",\"name\":\"OrderCreatedEvent\",\"namespace\":\"com.kafkalab.avro\",\"fields\":[{\"name\":\"eventVersion\",\"type\":\"string\",\"default\":\"1.0\"},{\"name\":\"orderId\",\"type\":\"string\",\"default\":\"\"},{\"name\":\"userId\",\"type\":\"string\",\"default\":\"\"},{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":\"com.kafkalab.avro.OrderItem\"},\"default\":[]},{\"name\":\"totalAmount\",\"type\":\"double\",\"default\":0.0},{\"name\":\"timestamp\",\"type\":\"string\",\"default\":\"\"},{\"name\":\"discountCode\",\"type\":\"string\",\"default\":\"\"}]}"
  }' | jq .
# Очікуємо: {"is_compatible":true}
```

## Еволюція схем

### Backward Compatible — додати поле з default ✅
```json
{"name": "discountCode", "type": "string", "default": ""}
```
- Старі consumers читають нові повідомлення: нове поле отримує default значення
- Schema Registry приймає нову версію схеми

### Incompatible — видалити поле без default ❌
Видалення поля `orderId` (без default) або зміна типу → Schema Registry відхиляє реєстрацію:
```json
{"error_code": 409, "message": "Schema being registered is incompatible with an earlier schema"}
```

### Режими сумісності
```bash
# Переглянути поточний режим
curl -s http://localhost:8125/config | jq .

# Змінити режим на FULL (найстрогіший)
curl -s -X PUT http://localhost:8125/config \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "FULL"}' | jq .
```

| Режим | Consumer читає старіші версії | Producer пише нові версії |
|-------|-------------------------------|---------------------------|
| BACKWARD | ✅ | — |
| FORWARD | — | ✅ |
| FULL | ✅ | ✅ |
| NONE | без обмежень | без обмежень |

## Порівняння розміру повідомлення

| Формат | Приблизний розмір для OrderCreatedEvent |
|--------|-----------------------------------------|
| JSON | ~300 байт (назви полів у кожному повідомленні) |
| Avro | ~80 байт (назви полів лише в схемі, payload — чисті байти) |

Avro додає 5 байт на початку кожного повідомлення: `0x00` (magic byte) + 4 байти schema ID.

## Стек

- Spring Boot 3.3.4 + Spring Kafka
- Apache Avro 1.11.4
- Confluent `kafka-avro-serializer` 7.7.1
- Gradle Avro Plugin `com.github.davidmc24.gradle.plugin.avro` 1.9.1
- Confluent Schema Registry 8.0.1