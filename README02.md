# Branch 02 — Topics & Partitions

## Що змінилося порівняно з branch01

| Аспект | branch01 | branch02 |
|--------|----------|----------|
| Кількість топіків | 1 (`orders.created`) | 4 топіки |
| Партиції у `orders.created` | 1 | **3** |
| Топік скасувань | відсутній | `orders.cancelled` (1 партиція) |
| Type headers | вимкнено | **увімкнено** + аліаси |
| Endpoint для скасування | відсутній | `POST /api/orders/{id}/cancel` |
| API лічильника | `{"received": N}` | `{"orders_created": N, "orders_cancelled": N, "total": N}` |
| Docker container names | `kafka`, `kafka-ui`, … | `kafka-b02`, `kafka-ui-b02`, … |
| Зовнішні порти | 9092, 8080, 8081, 8082 | **9094, 8083, 8091, 8092** |

---

## Архітектура

```
POST /api/orders
      │
      ▼
┌─────────────────┐   topic: orders.created (3 partitions)   ┌──────────────────────────┐
│  order-service  │  ──────────────────────────────────────▶  │                          │
│  :8091          │                                            │  notification-service    │
│                 │   topic: orders.cancelled (1 partition)   │  :8092                   │
│                 │  ──────────────────────────────────────▶  │                          │
└─────────────────┘                                           └──────────────────────────┘
         │                                                               │
         └──────────────────────┬────────────────────────────────────────┘
                                │
                   ┌────────────▼────────────┐
                   │      Apache Kafka       │
                   │      kafka-b02:9092     │
                   │      (KRaft mode)       │
                   └────────────┬────────────┘
                                │
                   ┌────────────▼────────────┐
                   │      Kafka UI           │
                   │      :8083              │
                   └─────────────────────────┘
```

### Топіки та їх налаштування

| Топік | Партиції | Retention | Призначення |
|-------|----------|-----------|-------------|
| `orders.created` | **3** | 7 днів | Нові замовлення — паралельна обробка |
| `orders.cancelled` | 1 | 7 днів | Скасування — строгий порядок важливий |
| `payments.processed` | **3** | 7 днів | Резерв для branch03+ |
| `notifications.sent` | 1 | 1 день | Підтвердження відправки — короткий retention |

---

## Ключові концепції цієї гілки

### Партиція (Partition)

Партиція — це впорядкована, незмінна послідовність записів всередині топіку.
Kafka ділить топік на N партицій і розподіляє їх між consumer-ами у групі.

```
orders.created
├── partition 0: msg[0], msg[1], msg[4], ...
├── partition 1: msg[2], msg[5], msg[8], ...
└── partition 2: msg[3], msg[6], msg[9], ...
```

**Навіщо 3 партиції для `orders.created`?**
- У branch04 ми запустимо 3 екземпляри notification-service — кожен отримає 1 партицію.
- Зараз (1 consumer) він читає всі 3 партиції сам.

### Офсет (Offset)

Кожне повідомлення в партиції має монотонно зростаючий офсет (0, 1, 2…).
Kafka UI показує офсет у колонці "Offset". Consumer group зберігає свій offset
у внутрішньому топіку `__consumer_offsets`.

### Retention

`retention.ms` визначає, скільки часу Kafka зберігає повідомлення після запису.
Після закінчення — повідомлення видаляються незалежно від того, чи їх прочитали.

```
orders.created  → 7 * 24 * 60 * 60 * 1000 = 604_800_000 ms = 7 днів
notifications.sent → 1 * 24 * 60 * 60 * 1000 = 86_400_000 ms = 1 день
```

### Type Headers (нове в branch02)

В branch01 producer надсилав чистий JSON без мета-заголовків (`add.type.headers=false`).
В branch02 увімкнено type headers з аліасами:

```
Producer заголовок:   __TypeId__ = "OrderCancelledEvent"
Consumer маппінг:     "OrderCancelledEvent" → com.kafkalab.notification.model.OrderCancelledEvent
```

Це дозволяє одному consumer слухати два топіки з різними типами повідомлень.

---

## Як запустити

### Варіант A — тільки branch02 (зупинити branch01 якщо запущено)

```bash
# зупинити branch01 (якщо запущено з іншого worktree або попереднього сеансу)
# docker compose -f ../kafka-laboratory/docker-compose.yml down   # або просто:
docker ps --format "{{.Names}}" | grep -E "kafka|order|notification" | xargs -r docker stop

# запустити branch02
docker compose up --build
```

### Варіант B — branch01 і branch02 одночасно

Порти не перетинаються, тому обидва стеки можуть жити одночасно:

```bash
# branch01 займає: 9092, 8080, 8081, 8082
# branch02 займає: 9094, 8083, 8091, 8092

docker compose up --build   # з папки kafka-laboratory (branch02)
```

Перевірити готовність:

```bash
docker compose ps
# kafka-b02, kafka-ui-b02, order-service-b02, notification-service-b02 — Running/healthy
```

---

## Як протестувати

### 1. Створити кілька замовлень (розподіл по партиціях)

```bash
for i in 1 2 3 4 5 6; do
  curl -s -X POST http://localhost:8091/api/orders \
    -H "Content-Type: application/json" \
    -d "{\"userId\":\"user-$i\",\"product\":\"Book $i\",\"quantity\":1,\"totalAmount\":$((i*10)).00}" | jq .orderId
done
```

Очікуваний результат у логах order-service-b02:

```
OrderCreated published → topic=orders.created, partition=0, offset=0, key=<uuid>
OrderCreated published → topic=orders.created, partition=2, offset=0, key=<uuid>
OrderCreated published → topic=orders.created, partition=1, offset=0, key=<uuid>
```

> Partition обирається за хешем ключа (`orderId`). Результати розподіляться між 0, 1, 2 — не обов'язково по черзі.

### 2. Скасувати замовлення

```bash
# підставте реальний orderId з відповіді попереднього запиту
ORDER_ID="f47ac10b-58cc-4372-a567-0e02b2c3d479"

curl -s -X POST "http://localhost:8091/api/orders/${ORDER_ID}/cancel" \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-1","reason":"Changed my mind"}' | jq
```

Відповідь:
```json
{
  "orderId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "userId": "user-1",
  "reason": "Changed my mind",
  "timestamp": "2026-06-11T12:00:00"
}
```

Лог order-service-b02:
```
OrderCancelled published → topic=orders.cancelled, partition=0, offset=0, key=f47ac10b-...
```

> `orders.cancelled` має 1 партицію — partition=0 завжди.

### 3. Перевірити лічильники

```bash
curl http://localhost:8092/api/notifications/count
```

```json
{
  "orders_created": 6,
  "orders_cancelled": 1,
  "total": 7
}
```

### 4. Переглянути топіки у Kafka UI

Відкрити у браузері: [http://localhost:8083](http://localhost:8083)

- **Topics** → побачите всі 4 топіки
- `orders.created` → вкладка **Partitions**: 3 партиції, у кожній свої офсети
- `orders.cancelled` → 1 партиція
- **Messages** → видно JSON + заголовок `__TypeId__`

### 5. CLI-команди всередині контейнера

```bash
# Список всіх топіків
docker exec kafka-b02 kafka-topics --bootstrap-server localhost:9092 --list

# Детальний опис (партиції, реплікація, лідер)
docker exec kafka-b02 kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic orders.created

# Перевірити retention
docker exec kafka-b02 kafka-configs --bootstrap-server localhost:9092 \
  --describe --entity-type topics --entity-name orders.created

# Статус consumer group (offset, lag, partition assignment)
docker exec kafka-b02 kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group notification-service-group
```

Очікуваний вивід `--describe --topic orders.created`:

```
Topic: orders.created   PartitionCount: 3   ReplicationFactor: 1
  Topic: orders.created  Partition: 0  Leader: 1  Replicas: 1  Isr: 1
  Topic: orders.created  Partition: 1  Leader: 1  Replicas: 1  Isr: 1
  Topic: orders.created  Partition: 2  Leader: 1  Replicas: 1  Isr: 1
```

---

## Як це працює всередині

### Producer (order-service) — що змінилося

#### KafkaTopicConfig.kt

В branch01 був один топік з 1 партицією. Тепер 4 топіки з явними налаштуваннями:

```kotlin
// 3 партиції + retention 7 днів
TopicBuilder.name("orders.created")
    .partitions(3)
    .replicas(1)
    .config(TopicConfig.RETENTION_MS_CONFIG, "604800000")
    .build()
```

Топіки `payments.processed` і `notifications.sent` поки не використовуються активно —
вони вже присутні в кластері і готові до branch03+.

#### OrderService.kt — type headers

В branch01: `spring.json.add.type.headers=false` — consumer знав тип через явну конфігурацію.
В branch02: `spring.json.add.type.headers=true` — кожне повідомлення несе заголовок `__TypeId__`.

Аліаси (type mapping) у producer:

```yaml
spring.json.type.mapping: "OrderCreatedEvent:com.kafkalab.order.model.OrderCreatedEvent,
                            OrderCancelledEvent:com.kafkalab.order.model.OrderCancelledEvent"
```

Замість `com.kafkalab.order.model.OrderCreatedEvent` у заголовку буде коротке `OrderCreatedEvent`.

#### OrderController.kt — новий endpoint

```
POST /api/orders/{orderId}/cancel
Body: { "userId": "...", "reason": "..." }
→ публікує OrderCancelledEvent у orders.cancelled
```

### Consumer (notification-service) — що змінилося

#### application.yml

В branch01 consumer десеріалізував `OrderCreatedEvent` за замовчуванням (`value.default.type`).
В branch02 тип визначається з заголовка + локальний маппінг:

```yaml
spring.json.type.mapping: "OrderCreatedEvent:com.kafkalab.notification.model.OrderCreatedEvent,
                            OrderCancelledEvent:com.kafkalab.notification.model.OrderCancelledEvent"
```

#### OrderEventListener.kt — два listener-и

```kotlin
@KafkaListener(topics = ["orders.created"])   // читає з 3 партицій
fun handleOrderCreated(record: ConsumerRecord<String, OrderCreatedEvent>)

@KafkaListener(topics = ["orders.cancelled"]) // читає з 1 партиції
fun handleOrderCancelled(record: ConsumerRecord<String, OrderCancelledEvent>)
```

Обидва listener-и належать до однієї `notification-service-group`.
Kafka вважає їх одним consumer-ом у групі.

---

## Структура проєкту (зміни відносно branch01)

```
kafka-laboratory/
├── docker-compose.yml             ← container names *-b02, порти 9094/8083/8091/8092
├── order-service/
│   └── src/main/kotlin/com/kafkalab/order/
│       ├── config/KafkaTopicConfig.kt    ← було 1 топік, стало 4 (з retention)
│       ├── controller/OrderController.kt ← новий POST /{id}/cancel
│       ├── model/
│       │   ├── CancelOrderRequest.kt     ← NEW
│       │   └── OrderCancelledEvent.kt    ← NEW
│       └── service/OrderService.kt       ← додано cancelOrder(), KafkaTemplate<String, Any>
│   └── src/main/resources/
│       └── application.yml              ← type.headers=true, type.mapping
└── notification-service/
    └── src/main/kotlin/com/kafkalab/notification/
        ├── controller/NotificationController.kt ← розбивка по топіках
        ├── listener/OrderEventListener.kt       ← два @KafkaListener
        └── model/
            └── OrderCancelledEvent.kt           ← NEW
    └── src/main/resources/
        └── application.yml                      ← type.mapping (без value.default.type)
```

---

## Ключові концепції цієї гілки

| Концепція | Що демонструє приклад |
|-----------|----------------------|
| **Partition** | `orders.created` має 3 партиції — видно в логах і Kafka UI |
| **Partition selection** | Key (orderId) → hash % 3 → різні partition для різних замовлень |
| **Offset** | Кожна партиція має власний offset-лічильник, починаючи з 0 |
| **Retention** | `orders.created` — 7 днів, `notifications.sent` — 1 день |
| **Naming conventions** | `<domain>.<event-type>`: `orders.created`, `payments.processed` |
| **Type headers** | `__TypeId__: OrderCancelledEvent` — consumer визначає клас за заголовком |
| **Multiple topics** | Один consumer group читає з двох топіків одночасно |
| **1 partition = strict order** | `orders.cancelled` — 1 партиція гарантує порядок скасувань |

---

## Що далі — branch03

У наступній гілці вивчаємо Message Keys детально:
- Як `hash(key) % numPartitions` вибирає партицію
- Чому всі події одного `userId` мають потрапляти в одну партицію
- `null` ключ → round-robin розподіл
- Порівняння: з ключем (`userId`) vs без ключа (`orderId`)