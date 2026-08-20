# Branch 02 — Topics & Partitions

## Що змінилося порівняно з branch01

| Аспект | branch01 | branch02 |
|--------|----------|----------|
| Кількість топіків | 1 (`01.orders.created`) | 4 топіки |
| Партиції у `orders.created` | 1 | **3** |
| Топік скасувань | відсутній | `02.orders.cancelled` (1 партиція) |
| Type headers | вимкнено | **увімкнено** + аліаси |
| Endpoint для скасування | відсутній | `POST /api/orders/{id}/cancel` |
| API лічильника | `{"received": N}` | `{"orders_created": N, "orders_cancelled": N, "total": N}` |
| Docker container names | `b01-order-service`, `b01-notification-service` | `b02-order-service`, `b02-notification-service` |
| Зовнішні порти сервісів | 8081, 8082 | **8091, 8092** |
| Kafka та Kafka UI | `kafka` (9092), `kafka-ui` (8080) | ті самі (спільні) |

---

## Архітектура

```
POST /api/orders
      │
      ▼
┌─────────────────┐   topic: 02.orders.created (3 partitions)    ┌──────────────────────────┐
│  order-service  │  ─────────────────────────────────────────▶  │                          │
│  :8091          │                                              │  notification-service    │
│                 │   topic: 02.orders.cancelled (1 partition)   │  :8092                   │
│                 │  ─────────────────────────────────────────▶  │                          │
└─────────────────┘                                              └──────────────────────────┘
         │                                                                  │
         └─────────────────────────┬────────────────────────────────────────┘
                                   │
                      ┌────────────▼────────────┐
                      │      Apache Kafka       │
                      │      kafka:9092         │
                      │      (KRaft mode)       │
                      └────────────┬────────────┘
                                   │
                      ┌────────────▼────────────┐
                      │      Kafka UI           │
                      │      :8080              │
                      └─────────────────────────┘
```

### Топіки та їх налаштування

| Топік | Партиції | Retention | Призначення |
|-------|----------|-----------|-------------|
| `02.orders.created` | **3** | 7 днів | Нові замовлення — паралельна обробка |
| `02.orders.cancelled` | 1 | 7 днів | Скасування — строгий порядок важливий |
| `02.payments.processed` | **3** | 7 днів | Резерв для branch03+ |
| `02.notifications.sent` | 1 | 1 день | Підтвердження відправки — короткий retention |

> Префікс `02.` дозволяє одночасно запускати branch01 та branch02 з одним Kafka кластером без конфліктів топіків.

---

## Ключові концепції цієї гілки

### Партиція (Partition)

Партиція — це впорядкована, незмінна послідовність записів всередині топіку.
Kafka ділить топік на N партицій і розподіляє їх між consumer-ами у групі.

```
02.orders.created
├── partition 0: msg[0], msg[1], msg[4], ...
├── partition 1: msg[2], msg[5], msg[8], ...
└── partition 2: msg[3], msg[6], msg[9], ...
```

**Навіщо 3 партиції для `02.orders.created`?**
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
02.orders.created    → 7 * 24 * 60 * 60 * 1000 = 604_800_000 ms = 7 днів
02.notifications.sent → 1 * 24 * 60 * 60 * 1000 = 86_400_000 ms = 1 день
```

### Type Headers (нове в branch02)

В branch01 producer надсилав чистий JSON без мета-заголовків (`spring.json.add.type.headers=false`).
В branch02 увімкнено type headers з аліасами:

```
Producer заголовок:   __TypeId__ = "OrderCancelledEvent"
Consumer маппінг:     "OrderCancelledEvent" → com.kafkalab.notification.model.OrderCancelledEvent
```

Це дозволяє одному consumer слухати два топіки з різними типами повідомлень.

---

## Як запустити

### Варіант A — тільки branch02

```bash
docker compose -f docker-compose-02.yml up --build
```

### Варіант B — branch01 і branch02 одночасно

Kafka і Kafka UI спільні для всіх гілок. Сервіси мають різні порти, тому обидва стеки сумісні.

```bash
# Спочатку запустити branch01 (kafka + kafka-ui вже стартують тут)
docker compose -f docker-compose-01.yml up --build

# В іншому терміналі — запустити тільки сервіси branch02
# (kafka вже запущена від branch01)
docker compose -f docker-compose-02.yml up --build --no-recreate order-service notification-service
```

> При запуску обох compose-файлів одночасно Docker побачить однакові container_name для `kafka` та `kafka-ui` і перевикористає їх, а не створить дублікати.

Перевірити готовність:

```bash
docker compose -f docker-compose-02.yml ps
# kafka, kafka-ui, b02-order-service, b02-notification-service — Running/healthy
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
або у Windows OS
```cmd
for i in 1 2 3 4 5 6; do
  curl -s -X POST http://localhost:8091/api/orders ^
    -H "Content-Type: application/json" ^
    -d "{\"userId\":\"user-$i\",\"product\":\"Book $i\",\"quantity\":1,\"totalAmount\":$((i*10)).00}"
done
```
Очікуваний результат у логах b02-order-service:

```
OrderCreated published → topic=02.orders.created, partition=0, offset=0, key=<uuid>
OrderCreated published → topic=02.orders.created, partition=2, offset=0, key=<uuid>
OrderCreated published → topic=02.orders.created, partition=1, offset=0, key=<uuid>
```

> Partition обирається за хешем ключа (`orderId`). Результати розподіляться між 0, 1, 2 — не обов'язково по черзі.

### 2. Скасувати замовлення

```bash
# підставте реальний orderId з відповіді попереднього запиту
ORDER_ID="ee5a4723-145f-4410-8dd9-72c9e83f1e86"

curl -s -X POST "http://localhost:8091/api/orders/${ORDER_ID}/cancel" \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-1","reason":"Changed my mind"}' | jq
```
або у Windows OS
```cmd
ORDER_ID="ee5a4723-145f-4410-8dd9-72c9e83f1e86"

curl -s -X POST "http://localhost:8091/api/orders/${ORDER_ID}/cancel" ^
  -H "Content-Type: application/json" ^
  -d '{"userId":"user-1","reason":"Changed my mind"}'
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

Лог b02-order-service:
```
OrderCancelled published → topic=02.orders.cancelled, partition=0, offset=0, key=f47ac10b-...
```

> `02.orders.cancelled` має 1 партицію — partition=0 завжди.

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

Відкрити у браузері: [http://localhost:8080](http://localhost:8080)

- **Topics** → побачите топіки `02.*` (і `01.*` якщо branch01 теж запущено)
- `02.orders.created` → вкладка **Partitions**: 3 партиції, у кожній свої офсети
- `02.orders.cancelled` → 1 партиція
- **Messages** → видно JSON + заголовок `__TypeId__`

### 5. CLI-команди всередині контейнера

```bash
# Список всіх топіків
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Детальний опис (партиції, реплікація, лідер)
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic 02.orders.created

# Перевірити retention
docker exec kafka kafka-configs --bootstrap-server localhost:9092 \
  --describe --entity-type topics --entity-name 02.orders.created

# Статус consumer group (offset, lag, partition assignment)
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group notification-service-group
```

Очікуваний вивід `--describe --topic 02.orders.created`:

```
Topic: 02.orders.created   PartitionCount: 3   ReplicationFactor: 1
  Topic: 02.orders.created  Partition: 0  Leader: 1  Replicas: 1  Isr: 1
  Topic: 02.orders.created  Partition: 1  Leader: 1  Replicas: 1  Isr: 1
  Topic: 02.orders.created  Partition: 2  Leader: 1  Replicas: 1  Isr: 1
```

---

## Як це працює всередині

### Producer (order-service) — що змінилося

#### KafkaTopicConfig.kt

В branch01 був один топік `01.orders.created` з 1 партицією. Тепер 4 топіки з явними налаштуваннями:

```kotlin
// 3 партиції + retention 7 днів
TopicBuilder.name("02.orders.created")
    .partitions(3)
    .replicas(1)
    .config(TopicConfig.RETENTION_MS_CONFIG, "604800000")
    .build()
```

Топіки `02.payments.processed` і `02.notifications.sent` поки не використовуються активно —
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
→ публікує OrderCancelledEvent у 02.orders.cancelled
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
@KafkaListener(topics = ["02.orders.created"])   // читає з 3 партицій
fun handleOrderCreated(record: ConsumerRecord<String, OrderCreatedEvent>)

@KafkaListener(topics = ["02.orders.cancelled"]) // читає з 1 партиції
fun handleOrderCancelled(record: ConsumerRecord<String, OrderCancelledEvent>)
```

Обидва listener-и належать до однієї `notification-service-group`.
Kafka вважає їх одним consumer-ом у групі.

---

## Структура проєкту (зміни відносно branch01)

```
kafka-laboratory/
├── docker-compose-02.yml                  ← container names b02-*, порти 8091/8092
├── branch02_topics_partitions/
│   ├── order-service/
│   │   └── src/main/kotlin/com/kafkalab/order/
│   │       ├── config/KafkaTopicConfig.kt    ← 4 топіки з префіксом 02. (з retention)
│   │       ├── controller/OrderController.kt ← новий POST /{id}/cancel
│   │       ├── model/
│   │       │   ├── CancelOrderRequest.kt     ← NEW
│   │       │   └── OrderCancelledEvent.kt    ← NEW
│   │       └── service/OrderService.kt       ← додано cancelOrder(), KafkaTemplate<String, Any>
│   │   └── src/main/resources/
│   │       └── application.yml              ← port 8091, type.headers=true, type.mapping
│   └── notification-service/
│       └── src/main/kotlin/com/kafkalab/notification/
│           ├── controller/NotificationController.kt ← розбивка по топіках
│           ├── listener/OrderEventListener.kt       ← два @KafkaListener
│           └── model/
│               └── OrderCancelledEvent.kt           ← NEW
│       └── src/main/resources/
│           └── application.yml                      ← port 8092, type.mapping
```

---

## Ключові концепції цієї гілки

| Концепція | Що демонструє приклад |
|-----------|----------------------|
| **Partition** | `02.orders.created` має 3 партиції — видно в логах і Kafka UI |
| **Partition selection** | Key (orderId) → hash % 3 → різні partition для різних замовлень |
| **Offset** | Кожна партиція має власний offset-лічильник, починаючи з 0 |
| **Retention** | `02.orders.created` — 7 днів, `02.notifications.sent` — 1 день |
| **Naming conventions** | `<branch>.<domain>.<event-type>`: `02.orders.created`, `02.payments.processed` |
| **Type headers** | `__TypeId__: OrderCancelledEvent` — consumer визначає клас за заголовком |
| **Multiple topics** | Один consumer group читає з двох топіків одночасно |
| **1 partition = strict order** | `02.orders.cancelled` — 1 партиція гарантує порядок скасувань |

---

## Що далі — branch03

У наступній гілці вивчаємо Message Keys детально:
- Як `hash(key) % numPartitions` вибирає партицію
- Чому всі події одного `userId` мають потрапляти в одну партицію
- `null` ключ → round-robin розподіл
- Порівняння: з ключем (`userId`) vs без ключа (`orderId`)

------------------------------------------

## Детальніше: навіщо Type Headers якщо можна просто два @KafkaListener?

### Чи реалізовано це в branch02?

Так. `OrderEventListener` має два методи з різними типами:

```kotlin
@KafkaListener(topics = ["02.orders.created"], groupId = "notification-service-group")
fun handleOrderCreated(record: ConsumerRecord<String, OrderCreatedEvent>)   // тип A

@KafkaListener(topics = ["02.orders.cancelled"], groupId = "notification-service-group")
fun handleOrderCancelled(record: ConsumerRecord<String, OrderCancelledEvent>) // тип B
```

І `application.yml` notification-service **не має** `spring.json.value.default.type`.
Це означає, що `JsonDeserializer` повністю покладається на заголовок `__TypeId__` кожного
повідомлення, щоб зрозуміти який клас інстанціювати.

### Чому не достатньо "однаковий groupId + два методи"?

Справа не в groupId і не в кількості методів — справа в тому, **як `JsonDeserializer` визначає тип**.

Сигнатура методу (`ConsumerRecord<String, OrderCreatedEvent>`) — це compile-time інформація.
`JsonDeserializer` — це окремий компонент, який працює **до** того як повідомлення потрапляє
до методу. Він не бачить сигнатуру — він бачить тільки байти і конфігурацію.

Без type headers є три варіанти:

| Підхід | Як працює | Проблема |
|--------|-----------|----------|
| `value.default.type` | один тип для всіх повідомлень у factory | не можна мати два різних типи в одному factory |
| Два окремих `KafkaListenerContainerFactory` | кожен factory має свій `JsonDeserializer` з конкретним типом | ручна конфігурація двох бінів + прив'язка до `@KafkaListener(containerFactory = "...")` |
| Десеріалізація у `Map<String, Any>` або `String` | немає потреби у типі | втрата типобезпеки, ручний парсинг |

З `spring.json.add.type.headers=true` producer додає `__TypeId__` до кожного повідомлення.
`JsonDeserializer` читає цей заголовок і сам обирає клас — **одна фабрика, будь-яка кількість типів**.

### Схема порівняння

```
БЕЗ type headers (branch01-стиль):
─────────────────────────────────
Producer: { JSON bytes }                 ← немає мета-інформації
Consumer JsonDeserializer: "який тип?"   ← шукає в конфігурації
  → value.default.type = OrderCreatedEvent  ← один на всіх, або
  → окремий factory per тип              ← ручна конфігурація

З type headers (branch02):
────────────────────────────
Producer: { JSON bytes } + header(__TypeId__ = "OrderCreatedEvent")
Consumer JsonDeserializer: "який тип?"
  → читає __TypeId__ = "OrderCreatedEvent"
  → шукає в type.mapping → com.kafkalab.notification.model.OrderCreatedEvent
  → інстанціює правильний клас ✓ (автоматично, без додаткових factory)
```

### Коли type headers особливо важливі

У branch02 кожен топік містить **один** тип подій, тому можна було б обійтись двома factory.
Але уявіть топік `domain.events` де в одній черзі йдуть `UserRegistered`, `UserUpdated`,
`UserDeleted` — тоді без type headers потрібен окремий factory для кожного з трьох типів,
або десеріалізація у загальний тип з ручним switch. З type headers — один factory, і кожне
повідомлення само каже "я є UserRegistered".

---