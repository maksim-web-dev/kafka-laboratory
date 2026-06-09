# Branch 01 — Basic Kafka in Docker

## Що реалізовано

Мінімальна але повноцінна система з двох Spring Boot сервісів, які спілкуються через Apache Kafka.

```
POST /api/orders
      │
      ▼
┌─────────────────┐        topic: orders.created        ┌──────────────────────────┐
│  order-service  │  ──────────────────────────────────▶ │  notification-service    │
│  :8081          │         (JSON message)               │  :8082                   │
└─────────────────┘                                      └──────────────────────────┘
         │                                                          │
         └─────────────────────┬────────────────────────────────────┘
                               │
                    ┌──────────▼──────────┐
                    │      Apache Kafka   │
                    │      (KRaft mode)   │
                    │         :9092       │
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │      Kafka UI       │
                    │         :8080       │
                    └─────────────────────┘
```

### Сервіси

| Сервіс | Порт | Роль |
|--------|------|------|
| `kafka` | 9092 | Брокер у KRaft-режимі (без Zookeeper) |
| `kafka-ui` | 8080 | Веб-інтерфейс для перегляду топіків і повідомлень |
| `order-service` | 8081 | Producer: приймає HTTP-запит, публікує `OrderCreatedEvent` |
| `notification-service` | 8082 | Consumer: читає `OrderCreatedEvent`, імітує відправку email |

---

## Як запустити

```bash
docker compose up --build
```

Перший запуск завантажує образи та компілює сервіси (5–7 хвилин).
Наступні запуски — швидкі.

Перевірити готовність:
```bash
docker compose ps
# всі 4 сервіси мають бути у стані "healthy" або "running"
```

---

## Як протестувати

### 1. Створити замовлення

```bash
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "user-42",
    "product": "Kafka: The Definitive Guide",
    "quantity": 2,
    "totalAmount": 59.99
  }' | jq
```

Відповідь:
```json
{
  "orderId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "userId": "user-42",
  "product": "Kafka: The Definitive Guide",
  "quantity": 2,
  "totalAmount": 59.99,
  "timestamp": "2026-06-10T12:00:00"
}
```

### 2. Перевірити логи notification-service

```bash
docker logs notification-service --tail=20
```

Очікуваний вивід:
```
──────────────────────────────────────
 NOTIFICATION #1
 Partition: 0  |  Offset: 0
 Order ID  : f47ac10b-58cc-4372-a567-0e02b2c3d479
 User ID   : user-42
 Product   : Kafka: The Definitive Guide x2
 Total     : $59.99
 Sent at   : 2026-06-10T12:00:00
 → Email sent to user user-42
──────────────────────────────────────
```

### 3. Перевірити лічильник отриманих повідомлень

```bash
curl http://localhost:8082/api/notifications/count
# {"received": 1}
```

### 4. Переглянути повідомлення у Kafka UI

Відкрити у браузері: [http://localhost:8080](http://localhost:8080)

- Topics → `orders.created` → Messages — видно JSON кожного повідомлення
- Consumers → `notification-service-group` — видно offset і lag

---

## Як це працює всередині

### Producer (order-service)

1. `OrderController` отримує POST-запит і передає дані в `OrderService`.
2. `OrderService` створює `OrderCreatedEvent` з унікальним `orderId` (UUID) та поточним timestamp.
3. `KafkaTemplate.send(topic, key, value)` серіалізує event у JSON і надсилає в топік `orders.created`.
   - **Key**: `orderId` — поки що не впливає на routing (1 партиція), але готує нас до branch03.
   - **Value**: JSON без type headers (`spring.json.add.type.headers=false`).
4. `CompletableFuture` callback логує partition і offset після підтвердження від брокера.

### Kafka (брокер)

- Працює в **KRaft mode**: замість Zookeeper використовує власний Raft-консенсус (`PROCESS_ROLES=broker,controller`).
- Топік `orders.created` створюється автоматично через Spring `NewTopic` bean при старті order-service.
- 1 партиція, replication factor 1 (single-broker setup для dev).

### Consumer (notification-service)

1. `@KafkaListener(topics = ["orders.created"], groupId = "notification-service-group")` підписується на топік при старті.
2. `auto-offset-reset: earliest` — якщо consumer group нова або offset не знайдено, читає з початку.
3. Метод отримує `ConsumerRecord<String, OrderCreatedEvent>` — включає metadata (partition, offset) і сам event.
4. `JsonDeserializer` з `spring.json.value.default.type` десеріалізує JSON у локальний `OrderCreatedEvent` клас.

### Чому два різних класи OrderCreatedEvent?

У `order-service` і `notification-service` є однойменний клас, але в різних пакетах.
Це свідомо: в реальній системі сервіси — незалежні застосунки, які не повинні ділити код через shared library (поки що).
Починаючи з branch11 (Avro + Schema Registry) ми вирішуємо цю проблему через контракти схем.

---

## Чому KRaft а не Zookeeper?

Apache Kafka 3.3+ підтримує **KRaft** (Kafka Raft) — вбудований механізм консенсусу.
З Kafka 4.0 Zookeeper повністю видалено. KRaft:
- Простіший деплой (один process замість двох)
- Швидший старт і shutdown
- Більш масштабований для великих кластерів

---

## Ключові концепції цієї гілки

| Концепція | Що демонструє приклад |
|-----------|----------------------|
| **Topic** | `orders.created` — канал для подій одного типу |
| **Producer** | `KafkaTemplate.send()` — публікація повідомлення |
| **Consumer** | `@KafkaListener` — підписка на топік |
| **Consumer Group** | `notification-service-group` — логічна група читачів |
| **Message Key** | `orderId` як ключ повідомлення |
| **Offset** | Порядковий номер повідомлення в партиції |
| **KRaft mode** | Kafka без Zookeeper |
| **JsonSerializer** | Серіалізація Kotlin data class у JSON |

---

## Структура проєкту

```
kafka-laboratory/
├── docker-compose.yml
├── order-service/
│   ├── Dockerfile
│   ├── build.gradle.kts
│   ├── settings.gradle.kts
│   └── src/main/kotlin/com/kafkalab/order/
│       ├── OrderServiceApplication.kt
│       ├── config/KafkaTopicConfig.kt      ← створення топіку
│       ├── controller/OrderController.kt   ← POST /api/orders
│       ├── model/
│       │   ├── CreateOrderRequest.kt
│       │   └── OrderCreatedEvent.kt
│       └── service/OrderService.kt         ← KafkaTemplate.send()
└── notification-service/
    ├── Dockerfile
    ├── build.gradle.kts
    ├── settings.gradle.kts
    └── src/main/kotlin/com/kafkalab/notification/
        ├── NotificationServiceApplication.kt
        ├── controller/NotificationController.kt  ← GET /api/notifications/count
        ├── listener/OrderEventListener.kt        ← @KafkaListener
        └── model/OrderCreatedEvent.kt
```

---

## Що далі — branch02

У наступній гілці вивчаємо топіки детальніше:
- Декілька топіків для різних типів подій
- Naming conventions
- Налаштування партицій і retention через CLI
- `kafka-topics.sh` команди