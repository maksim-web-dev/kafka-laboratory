# Branch 02  — Topics & Partitions

---

## `branch02_topics_partitions` — що вивчаємо

- Розширення до 4 топіків з префіксом `02.` (розрізнення топіків між гілками в одному кластері)
- Топік `02.orders.created` з 3 партиціями — паралельна обробка
- Топік `02.orders.cancelled` з 1 партицією — суворий порядок
- Налаштування `retention.ms` (`604_800_000` — 7 днів, `86_400_000` — 1 день)
- `spring.json.add.type.headers=true` — type headers `__TypeId__` у повідомленнях
- `spring.json.type.mapping` — аліаси замість FQCN у заголовку
- Два `@KafkaListener` для двох різних типів подій в одній consumer group
- CLI-команди: `kafka-topics --describe`, `kafka-consumer-groups --describe`
- Концепції: Partition, Partition selection (hash % N), Offset, Retention, Naming conventions

---

## Що змінилося порівняно з branch01

- Кількість топіків: 4 замість 1
- Кількість партицій: 3 замість 1
- Запроважден топік скасувань (`02.orders.cancelled` (1 партиція))
- Запроважден Type headers + аліаси
- Запроважден Endpoint для скасування (`POST /api/orders/{id}/cancel`)
- Розширен API лічильника (`{"orders_created": N, "orders_cancelled": N, "total": N}`)

---

## Архітектура

```
POST /api/orders
      │
      ▼
┌─────────────────┐   topic: 02.orders.created (3 partitions)    ┌──────────────────────────┐
│  order-service  │  ─────────────────────────────────────────▶  │                          │
│  :8081          │                                              │  notification-service    │
│                 │   topic: 02.orders.cancelled (1 partition)   │  :8082                   │
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

- **`02.orders.created`** — 3 партиції, retention 7 днів — нові замовлення, паралельна обробка
- **`02.orders.cancelled`** — 1 партиція, retention 7 днів — скасування, строгий порядок важливий
- **`02.payments.processed`** — 3 партиції, retention 7 днів — резерв для branch03+
- **`02.notifications.sent`** — 1 партиція, retention 1 день — підтвердження відправки, короткий retention

> Префікс `02.` відокремлює топіки цієї гілки від інших — при перемиканні між гілками в одному Kafka кластері топіки не перетинаються.

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

```bash
docker compose -f docker-compose-02.yml up --build
```

Перевірити готовність:

```bash
docker compose -f docker-compose-02.yml ps
# kafka, kafka-ui, order-service, notification-service — Running/healthy
```

---

## Як протестувати

### 1. Створити кілька замовлень (розподіл по партиціях)

```bash
for i in 1 2 3 4 5 6; do
  curl -s -X POST http://localhost:8081/api/orders \
    -H "Content-Type: application/json" \
    -d "{\"userId\":\"user-$i\",\"product\":\"Book $i\",\"quantity\":1,\"totalAmount\":$((i*10)).00}" | jq .orderId
done
```
або у Windows OS
```cmd
for i in 1 2 3 4 5 6; do
  curl -s -X POST http://localhost:8081/api/orders ^
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

curl -s -X POST "http://localhost:8081/api/orders/${ORDER_ID}/cancel" \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-1","reason":"Changed my mind"}' | jq
```
або у Windows OS
```cmd
ORDER_ID="ee5a4723-145f-4410-8dd9-72c9e83f1e86"

curl -s -X POST "http://localhost:8081/api/orders/${ORDER_ID}/cancel" ^
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
curl http://localhost:8082/api/notifications/count
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

- **Topics** → побачите топіки `02.*`
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
├── docker-compose-02.yml                  ← порти 8081/8082 (ті ж, що у branch01)
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
│   │       └── application.yml              ← port 8081, type.headers=true, type.mapping
│   └── notification-service/
│       └── src/main/kotlin/com/kafkalab/notification/
│           ├── controller/NotificationController.kt ← розбивка по топіках
│           ├── listener/OrderEventListener.kt       ← два @KafkaListener
│           └── model/
│               └── OrderCancelledEvent.kt           ← NEW
│       └── src/main/resources/
│           └── application.yml                      ← port 8082, type.mapping
```

---

## Ключові концепції цієї гілки

- **Partition** — `02.orders.created` має 3 партиції, видно в логах і Kafka UI
- **Partition selection** — Key (orderId) → hash % 3 → різні partition для різних замовлень
- **Offset** — кожна партиція має власний offset-лічильник, починаючи з 0
- **Retention** — `02.orders.created` 7 днів, `02.notifications.sent` 1 день
- **Naming conventions** — `<branch>.<domain>.<event-type>`: `02.orders.created`, `02.payments.processed`
- **Type headers** — `__TypeId__: OrderCancelledEvent`, consumer визначає клас за заголовком
- **Multiple topics** — один consumer group читає з двох топіків одночасно
- **1 partition = strict order** — `02.orders.cancelled`, 1 партиція гарантує порядок скасувань

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

- **`value.default.type`** — один тип для всіх повідомлень у factory; проблема: не можна мати два різних типи в одному factory
- **Два окремих `KafkaListenerContainerFactory`** — кожен factory має свій `JsonDeserializer` з конкретним типом; проблема: ручна конфігурація двох бінів + прив'язка до `@KafkaListener(containerFactory = "...")`
- **Десеріалізація у `Map<String, Any>` або `String`** — немає потреби у типі; проблема: втрата типобезпеки, ручний парсинг

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
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------

## Слайди для презентації — Branch 02: Topics & Partitions

---

### Слайд 1 — Заголовок

**Branch 02: Topics & Partitions**

- Партиції та паралельна обробка
- Офсети та retention
- Type headers для багатотипних consumer-ів
- Naming conventions у продакшн-системах

---

### Слайд 2 — Що було в branch01 і чому цього мало

**Branch01: одна партиція — один потік**

```
1 топік → 1 партиція → 1 consumer → послідовна обробка
```

- Максимальна пропускна здатність = швидкість одного consumer-а
- Один тип повідомлень — один listener
- Немає ізоляції між типами подій (created vs cancelled)

**Рішення branch02:** кілька партицій + окремі топіки для різних типів подій

---

### Слайд 3 — Що таке партиція

**Партиція = впорядкована черга всередині топіку**

```
02.orders.created  (3 партиції)
├── partition 0:  msg[offset=0], msg[offset=3], msg[offset=6] ...
├── partition 1:  msg[offset=0], msg[offset=2], msg[offset=5] ...
└── partition 2:  msg[offset=0], msg[offset=1], msg[offset=4] ...
```

- Kafka гарантує порядок **тільки всередині** однієї партиції
- Між партиціями — порядок не визначено
- Кожна партиція читається рівно одним consumer-ом у групі

---

### Слайд 4 — Як обирається партиція

**Правило: `partition = hash(key) % numPartitions`**

```
key = "order-uuid-123"  →  hash = 2847162  →  2847162 % 3 = 0  →  partition 0
key = "order-uuid-456"  →  hash = 9183714  →  9183714 % 3 = 2  →  partition 2
key = null              →  round-robin  →  0, 1, 2, 0, 1, 2 ...
```

- Один і той самий ключ **завжди** потрапляє в одну партицію
- Це гарантує порядок для всіх подій одного об'єкта (замовлення, користувача)
- `null`-ключ → рівномірний розподіл, але без гарантії порядку

---

### Слайд 5 — Офсет (Offset)

**Офсет = монотонний номер повідомлення в партиції**

```
partition 0:  [0] [1] [2] [3] [4] ...
                           ↑
                    consumer offset = 3 (наступне до читання — [3])
```

- Consumer group зберігає свій offset у топіку `__consumer_offsets`
- При перезапуску — продовжує з того ж місця
- Можна перемотати offset назад (`--reset-offsets`) для повторної обробки
- Kafka UI показує: `LOG-END-OFFSET`, `CURRENT-OFFSET`, `LAG`

---

### Слайд 6 — Retention: скільки живуть повідомлення

**`retention.ms` = час зберігання після запису**

```
02.orders.created    → 7 днів  (604_800_000 ms)
02.notifications.sent → 1 день  (86_400_000 ms)
```

- Повідомлення видаляється **після закінчення часу**, незалежно від того, чи його прочитали
- Якщо consumer "відстав" більш ніж на retention — він втратить повідомлення
- Також є `retention.bytes` — обмеження за розміром партиції

---

### Слайд 7 — 1 партиція = строгий порядок

**Коли порядок критичний — використовуємо 1 партицію**

```
02.orders.cancelled  (1 партиція)
→ partition 0 завжди
→ скасування обробляються строго в порядку надходження
```

- Скасування #1 завжди буде оброблено до скасування #2
- Якщо партицій > 1 — два скасування одного замовлення можуть прийти в різному порядку
- Компроміс: пропускна здатність обмежена одним consumer-ом

---

### Слайд 8 — Naming conventions

**Формат топіку: `<prefix>.<domain>.<event-type>`**

```
02.orders.created
02.orders.cancelled
02.payments.processed
02.notifications.sent
│    │         │
│    │         └── тип події (created, cancelled, processed)
│    └────────────── домен (orders, payments, notifications)
└─────────────────── префікс гілки / середовища
```

У продакшні замість номера гілки використовують середовище: `prod.`, `staging.`, `dev.`

---

### Слайд 9 — Type Headers: проблема

**Як JsonDeserializer визначає тип без headers?**

```
Варіант A: value.default.type = OrderCreatedEvent
           → лише один тип для всього factory

Варіант B: два окремих KafkaListenerContainerFactory
           → ручна конфігурація, прив'язка через containerFactory="..."

Варіант C: десеріалізація у Map<String,Any>
           → втрата типобезпеки
```

Жоден варіант не масштабується при 5+ типах подій в одному сервісі.

---

### Слайд 10 — Type Headers: рішення

**`spring.json.add.type.headers=true` → `__TypeId__` у заголовку**

```
Producer надсилає:
  { JSON bytes }  +  header: __TypeId__ = "OrderCreatedEvent"

Consumer JsonDeserializer:
  читає __TypeId__  →  шукає в type.mapping
  "OrderCreatedEvent" → com.kafkalab.notification.model.OrderCreatedEvent
  → інстанціює правильний клас автоматично
```

**Один factory — будь-яка кількість типів повідомлень.**

Аліаси (`OrderCreatedEvent` замість FQCN) прибирають залежність від імені пакету між сервісами.

---

### Слайд 11 — Підсумки branch02

**Що вивчили:**

- Партиція — одиниця паралелізму і порядку в Kafka
- `hash(key) % N` визначає партицію; `null`-ключ — round-robin
- Офсет — позиція consumer-а; зберігається у `__consumer_offsets`
- `retention.ms` — час life повідомлення, не залежить від прочитання
- 1 партиція = строгий порядок; N партицій = паралельність
- Type headers — масштабований спосіб десеріалізації різних типів
- Naming convention: `<env>.<domain>.<event-type>`

**Далі — branch03:** Message Keys детально: чому `userId` краще за `orderId` як ключ.

---

---------------------------------------------------------------------------------------------------------

## Текст для презентації — Branch 02: Topics & Partitions

### Вступ (Слайд 1)

У першій гілці ми запустили найпростішу Kafka-систему: один топік, одна партиція, один producer, один consumer.
Це чудово для знайомства, але не відображає реальні системи.
У branch02 ми зробимо крок до продакшн-патернів: розберемося з партиціями, офсетами, retention і навчимося правильно називати топіки.

---

### Проблема однієї партиції (Слайд 2)

Уявіть інтернет-магазин в Black Friday. Тисячі замовлень за хвилину.
Якщо у вас один топік з однією партицією — вся обробка послідовна. Один consumer читає одне повідомлення за раз.
Масштабувати не можна: другий consumer у тій самій групі просто сидітиме без роботи — йому нічого читати.
Партиції — це рішення. Три партиції = три consumer-и можуть читати паралельно.

---

### Що таке партиція (Слайд 3)

Топік — це логічний контейнер. Фізично він складається з партицій.
Кожна партиція — це впорядкована, незмінна послідовність повідомлень. Як лог-файл.
Важливо: Kafka гарантує порядок тільки всередині однієї партиції. Між партиціями порядку немає.
Ось чому вибір кількості партицій і ключа повідомлення — це архітектурне рішення.

---

### Як обирається партиція (Слайд 4)

Коли producer надсилає повідомлення з ключем, Kafka обчислює: `partition = hash(key) % numPartitions`.
Це детерміновано: один і той самий ключ завжди потрапляє в одну й ту саму партицію.
В нашому прикладі ключ — це `orderId`. Тому всі події одного замовлення гарантовано потраплять в одну партицію — і будуть оброблені в правильному порядку.
Якщо ключа немає (`null`) — round-robin: повідомлення рівномірно розподіляються між партиціями, але порядку між ними немає.

---

### Офсет (Слайд 5)

Кожне повідомлення в партиції отримує монотонно зростаючий номер — офсет. 0, 1, 2, 3...
Consumer group запам'ятовує, яке повідомлення вона вже обробила — зберігає "поточний офсет" у спеціальному внутрішньому топіку `__consumer_offsets`.
Якщо consumer впав і перезапустився — він читає офсет з `__consumer_offsets` і продовжує з того місця, де зупинився.
Це і є надійність Kafka: at-least-once delivery за замовчуванням.
Різниця між поточним офсетом і кінцем партиції — це "lag". Якщо lag зростає — consumer не встигає.

---

### Retention (Слайд 6)

Kafka — це не черга, яка видаляє повідомлення одразу після прочитання. Це лог.
`retention.ms` визначає, скільки часу повідомлення живе. Після — видаляється назавжди.
Для замовлень ми зберігаємо 7 днів: можна заново програти події, налагодити систему, відновитись після збою consumer-а.
Для підтверджень нотифікацій — 1 день достатньо: якщо не обробили за день, це вже неактуально.
Зверніть увагу: видалення відбувається незалежно від того, чи прочитали повідомлення. Якщо consumer відстав більше ніж на retention — він пропустить ці повідомлення.

---

### 1 партиція і строгий порядок (Слайд 7)

Топік `02.orders.cancelled` має рівно одну партицію. Чому?
Порядок скасувань критичний. Якщо користувач скасував замовлення двічі (retry) — ми маємо обробити перше скасування раніше другого.
З однією партицією це гарантовано: всі повідомлення читаються строго послідовно.
З трьома партиціями — два скасування одного замовлення можуть потрапити в різні партиції і бути оброблені в іншому порядку.
Компроміс: одна партиція = один consumer = менша пропускна здатність. Але для скасувань це прийнятно.

---

### Naming conventions (Слайд 8)

Правильне іменування топіків — це основа підтримуваної системи.
Наш формат: `<prefix>.<domain>.<event-type>`.
Префікс `02.` — це номер гілки у нашому навчальному контексті. У продакшні це буде `prod.`, `staging.`, або назва команди.
Домен — бізнес-область: `orders`, `payments`, `notifications`.
Тип події — що сталося: `created`, `cancelled`, `processed`, `sent`.
Дивлячись на назву топіку `prod.orders.created` — відразу зрозуміло: продакшн, замовлення, подія створення.

---

### Type Headers (Слайди 9–10)

У branch01 ми мали один топік і один тип повідомлень — `OrderCreatedEvent`. Consumer знав тип заздалегідь через `value.default.type`.
У branch02 маємо два топіки і два типи: `OrderCreatedEvent` і `OrderCancelledEvent`. Як deserializer зрозуміє, який клас створювати?
Варіант "два окремих factory" — він працює, але не масштабується. 10 типів = 10 factory, 10 `@KafkaListener(containerFactory="...")`.
Рішення: `spring.json.add.type.headers=true`. Producer додає до кожного повідомлення заголовок `__TypeId__` зі значенням-аліасом типу.
Consumer зчитує цей заголовок, шукає у `type.mapping` відповідний клас і автоматично інстанціює його.
Аліаси — щоб не прив'язуватись до конкретного пакету. Якщо ми перейменуємо пакет — змінимо лише маппінг, не торкаючись producer-а.

---

### Підсумки (Слайд 11)

Сьогодні ми розібрали фундаментальні концепції, без яких не обходиться жоден Kafka-проєкт.
Партиції — це не просто "більше throughput". Це архітектурне рішення про порядок і паралелізм.
Офсети дають нам надійність: можна перепрочитати, відновитись, реплеїти події.
Retention — свідоме рішення: скільки часу бізнес-подія має залишатись доступною.
Type headers — production-ready підхід до десеріалізації різнотипних повідомлень.
У наступній гілці заглибимося у Message Keys: чому вибір ключа — це бізнес-рішення, а не технічна деталь.

---

---------------------------------------------------------------------------------------------------------

## Тести — Branch 02: Topics & Partitions

---

**Питання 1.**
Що таке партиція (partition) в Apache Kafka?

- A) Окремий Kafka-брокер у кластері
- B) Впорядкована послідовність повідомлень всередині топіку
- C) Consumer group, яка читає з одного топіку
- D) Мережевий розділ між producer і consumer

**Правильна відповідь: B**
Партиція — це впорядкована, незмінна послідовність (лог) повідомлень. Топік складається з однієї або більше партицій.

---

**Питання 2.**
Kafka гарантує порядок повідомлень:

- A) Між усіма партиціями топіку
- B) Тільки всередині однієї партиції
- C) У межах однієї consumer group
- D) Між усіма топіками одного брокера

**Правильна відповідь: B**
Порядок гарантовано лише всередині партиції. Між різними партиціями порядок не визначено.

---

**Питання 3.**
Producer надсилає повідомлення з ключем `"user-42"` у топік з 4 партиціями. За яким принципом обирається партиція?

- A) Випадково
- B) Round-robin між усіма партиціями
- C) `hash("user-42") % 4`
- D) Завжди partition 0

**Правильна відповідь: C**
При наявності ключа Kafka використовує DefaultPartitioner: `murmur2_hash(key) % numPartitions`. Один і той самий ключ завжди потрапляє в одну партицію.

---

**Питання 4.**
Producer надсилає повідомлення без ключа (`key = null`). Яка стратегія розподілу за замовчуванням?

- A) Завжди partition 0
- B) Хеш від значення повідомлення
- C) Round-robin або sticky partitioning
- D) Випадкова партиція, нова для кожного повідомлення

**Правильна відповідь: C**
При `null`-ключі Kafka використовує round-robin або (починаючи з Kafka 2.4) sticky partitioning — накопичує повідомлення в одній партиції до заповнення батчу, потім переходить на іншу.

---

**Питання 5.**
Що таке офсет (offset) у Kafka?

- A) Затримка між producer-ом і consumer-ом у мілісекундах
- B) Монотонно зростаючий номер повідомлення в межах партиції
- C) Позиція брокера в кластері
- D) Кількість повідомлень у топіку

**Правильна відповідь: B**
Офсет — унікальний порядковий номер повідомлення всередині конкретної партиції. Починається з 0, зростає монотонно.

---

**Питання 6.**
Де Kafka зберігає поточний офсет consumer group?

- A) У ZooKeeper
- B) У файловій системі брокера
- C) У внутрішньому топіку `__consumer_offsets`
- D) У пам'яті consumer-а

**Правильна відповідь: C**
Починаючи з Kafka 0.9, офсети зберігаються у внутрішньому топіку `__consumer_offsets`, а не в ZooKeeper.

---

**Питання 7.**
Що відбувається з повідомленням після закінчення `retention.ms`?

- A) Воно переміщується в архів
- B) Воно позначається як "прочитане" і залишається
- C) Воно видаляється незалежно від того, чи його прочитали
- D) Воно переміщується до наступної партиції

**Правильна відповідь: C**
Retention — це TTL для повідомлень. Kafka видаляє їх після закінчення часу, навіть якщо consumer ще не прочитав їх.

---

**Питання 8.**
Скільки consumer-ів з однієї consumer group можуть читати одну партицію одночасно?

- A) Необмежено
- B) Рівно 2 для відмовостійкості
- C) Рівно 1
- D) Залежить від `max.poll.records`

**Правильна відповідь: C**
Kafka гарантує, що одну партицію в межах consumer group читає рівно один consumer. Це основа моделі паралелізму Kafka.

---

**Питання 9.**
У consumer group 2 consumer-и, топік має 5 партицій. Як розподіляться партиції?

- A) Кожен consumer читає всі 5 партицій
- B) Один отримає 3 партиції, інший — 2
- C) Один отримає всі 5, інший буде idle
- D) Kafka поверне помилку — кількість consumer-ів має дорівнювати кількості партицій

**Правильна відповідь: B**
Kafka розподіляє партиції рівномірно: при 5 партиціях і 2 consumer-ах розподіл буде 3+2 або 2+3.

---

**Питання 10.**
Consumer group має 4 consumer-и, але топік має лише 3 партиції. Що станеться з четвертим consumer-ом?

- A) Він читатиме всі партиції як резервний
- B) Він залишиться idle (не отримає жодної партиції)
- C) Kafka автоматично збільшить кількість партицій до 4
- D) Він читатиме ту саму партицію, що й третій consumer

**Правильна відповідь: B**
Consumer-ів не може бути корисно більше, ніж партицій. Зайві consumer-и залишаються без роботи. Саме тому кількість партицій визначає максимальний ступінь паралелізму.

---

**Питання 11.**
Навіщо для топіку `02.orders.cancelled` обрано 1 партицію?

- A) Щоб зекономити місце на диску
- B) Щоб гарантувати строгий порядок обробки скасувань
- C) Тому що Kafka не підтримує більше 1 партиції для цього типу топіків
- D) Щоб спростити конфігурацію consumer-а

**Правильна відповідь: B**
З однією партицією Kafka гарантує, що всі скасування обробляються строго у порядку надходження. З кількома партиціями два скасування одного замовлення могли б бути оброблені в невірному порядку.

---

**Питання 12.**
Що таке "consumer lag"?

- A) Час затримки між producer і Kafka-брокером
- B) Різниця між поточним офсетом consumer-а і останнім офсетом у партиції
- C) Кількість повідомлень, відправлених producer-ом за секунду
- D) Час повторного підключення consumer-а після збою

**Правильна відповідь: B**
Lag = `LOG-END-OFFSET - CURRENT-OFFSET`. Показує, скільки повідомлень consumer "відстав" від реальних даних. Зростаючий lag — сигнал тривоги.

---

**Питання 13.**
Що робить налаштування `spring.json.add.type.headers=true`?

- A) Додає HTTP-заголовки до Kafka-повідомлень
- B) Змушує consumer перевіряти тип перед десеріалізацією
- C) Producer додає заголовок `__TypeId__` із назвою Java-класу до кожного повідомлення
- D) Включає шифрування повідомлень за типом

**Правильна відповідь: C**
При `true` Spring Kafka producer додає до кожного повідомлення заголовок `__TypeId__`, значення якого — повне ім'я класу або аліас із `type.mapping`.

---

**Питання 14.**
Навіщо потрібен `spring.json.type.mapping` ("OrderCreatedEvent:com.kafkalab...OrderCreatedEvent")?

- A) Щоб зменшити розмір повідомлення
- B) Щоб відв'язати назву класу у заголовку від конкретного пакету
- C) Щоб увімкнути компресію повідомлень
- D) Щоб налаштувати пріоритет обробки типів

**Правильна відповідь: B**
Аліаси дозволяють producer-у і consumer-у мати різні пакетні структури. У заголовку зберігається короткий аліас `OrderCreatedEvent`, а не `com.kafkalab.order.model.OrderCreatedEvent` — якщо пакет зміниться, достатньо оновити маппінг.

---

**Питання 15.**
Consumer group має один `KafkaListenerContainerFactory` і два `@KafkaListener` для різних топіків з різними типами подій. Без type headers що стане проблемою?

- A) Два listener-и не можуть бути в одній consumer group
- B) `JsonDeserializer` не зможе визначити тип без зовнішньої підказки і використає `value.default.type` — один на всі повідомлення
- C) Kafka не дозволить підключитись до двох топіків одночасно
- D) Consumer отримає `ClassCastException` на кожному другому повідомленні

**Правильна відповідь: B**
`JsonDeserializer` не бачить сигнатуру методу — він бачить байти. Без `__TypeId__` у заголовку він може використати лише `value.default.type`, що дає один фіксований тип для всіх повідомлень.

---

**Питання 16.**
У якому форматі рекомендовано іменувати топіки в продакшн-системі?

- A) Випадкові UUID для уникнення конфліктів
- B) `<env>.<domain>.<event-type>`, наприклад `prod.orders.created`
- C) Лише назва мікросервісу, наприклад `order-service`
- D) Номер версії API, наприклад `v1-orders`

**Правильна відповідь: B**
Загальноприйнятий патерн: середовище, домен, тип події. Дозволяє одночасно мати `prod.orders.created` і `staging.orders.created` в одному кластері без конфліктів.

---

**Питання 17.**
Retention налаштований на 7 днів. Consumer не читав топік 10 днів. Що трапиться після відновлення?

- A) Consumer прочитає всі 10 днів повідомлень
- B) Consumer отримає помилку і не зможе підключитись
- C) Повідомлення за перші 3 дні будуть втрачені, consumer почне з найстарішого доступного
- D) Kafka автоматично збільшить retention до 10 днів

**Правильна відповідь: C**
Повідомлення старші за `retention.ms` видаляються. Consumer отримає `OffsetOutOfRangeException` або буде переведений на `earliest` доступний офсет — залежно від `auto.offset.reset`.

---

**Питання 18.**
Яку команду CLI використати, щоб перевірити lag consumer group `notification-service-group`?

- A) `kafka-topics --describe --group notification-service-group`
- B) `kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group notification-service-group`
- C) `kafka-offsets --list --group notification-service-group`
- D) `kafka-consumer-groups --lag --group notification-service-group`

**Правильна відповідь: B**
`kafka-consumer-groups --describe` показує для кожної партиції: `CURRENT-OFFSET`, `LOG-END-OFFSET`, `LAG`, `CONSUMER-ID`, `HOST`.

---

**Питання 19.**
Що покаже команда `kafka-topics --describe --topic 02.orders.created`?

- A) Список усіх повідомлень у топіку
- B) Кількість партицій, replication factor, лідер-брокер для кожної партиції та конфіги топіку
- C) Поточні офсети всіх consumer groups
- D) Статистику throughput за останні 24 години

**Правильна відповідь: B**
`--describe` виводить метадані топіку: `PartitionCount`, `ReplicationFactor` і для кожної партиції — `Leader`, `Replicas`, `Isr` та overridden конфіги (наприклад, `retention.ms`).

---

**Питання 20.**
Є топік `domain.events` де в одній черзі йдуть події `UserRegistered`, `UserUpdated`, `UserDeleted`. Який підхід десеріалізації є найбільш масштабованим?

- A) Три окремих `KafkaListenerContainerFactory` — по одному на кожен тип
- B) Десеріалізація у `String`, потім ручний `ObjectMapper.readValue()` з перевіркою поля `type`
- C) `spring.json.add.type.headers=true` з `type.mapping` — один factory, тип визначається з заголовка `__TypeId__`
- D) Один `@KafkaListener` з параметром `Map<String, Any>` і switch за ключем

**Правильна відповідь: C**
Type headers — найчистіший підхід: один factory, автоматична десеріалізація у потрібний клас, не потрібно змінювати consumer при додаванні нового типу — лише додати новий рядок у `type.mapping`.
