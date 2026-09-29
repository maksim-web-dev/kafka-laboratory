# Branch 10 — JSON Serialization

## `branch10_json_serialization` — що вивчаємо

- **`JsonSerializer` / `JsonDeserializer`** — Spring Kafka вбудована серіалізація через Jackson.
- **`__TypeId__` header** — Kafka header з інформацією про тип класу для десеріалізації.
- **`spring.json.add.type.headers: true`** — producer додає `__TypeId__` до кожного повідомлення.
- **`spring.json.type.mapping`** — alias замість FQCN для безпечного cross-service маппінгу.
- **`spring.json.trusted.packages`** — whitelist пакетів для безпечної десеріалізації.
- **Event versioning** — поле `eventVersion` у моделі для backward compatibility.
- **Nested objects** — `List<OrderItem>` всередині `OrderCreatedEvent` (серіалізується автоматично).
- **`PaymentProcessedEvent`** — новий тип події зі статусом `APPROVED` / `DECLINED`.
- **Multi-topic pipeline** — тристоронній ланцюг: `orders.created` → payment → `payments.processed` → notification.
- **DLT → business event** — `@DltHandler` публікує `PaymentProcessedEvent(DECLINED)` замість тихого логування.

## Що змінилося порівняно з branch09

- Повернуто `notification-service` — тепер тристоронній pipeline.
- Модель `OrderCreatedEvent` розширена: `+eventVersion`, `+items: List<OrderItem>`.
- Доданий новий тип `OrderItem` (вкладений об'єкт).
- Новий топік `10.payments.processed` — для комунікації payment → notification.
- Новий тип події `PaymentProcessedEvent` зі статусом `APPROVED`/`DECLINED`.
- `@DltHandler` тепер публікує `PaymentProcessedEvent(DECLINED)` замість тихого логування.
- `notification-service` слухає 3 топіки: `orders.created`, `orders.cancelled`, `payments.processed`.
- `spring.json.type.mapping` налаштовано у всіх сервісах для безпечного маппінгу.
- Назви топіків змінено на `10.orders.*`, `10.payments.*`.

## Архітектура

```
order-service-b10 :8081
  createOrder() → OrderCreatedEvent{eventVersion, items: [OrderItem]}
          │
     10.orders.created (3 partitions)
     10.orders.cancelled (1 partition)
          │                   │
          ▼                   ▼
payment-service-b10 :8083    notification-service-b10 :8082
  @RetryableTopic             handleOrderCancelled()
  handleOrderCreated()
    APPROVED → PaymentProcessedEvent
    DECLINED → @DltHandler → PaymentProcessedEvent(DECLINED)
          │
     10.payments.processed (3 partitions)
          │
          ▼
notification-service-b10 :8082
  handlePaymentProcessed()
  логує APPROVED / DECLINED

__TypeId__ header в кожному повідомленні:
  10.orders.created   → __TypeId__: OrderCreatedEvent
  10.payments.processed → __TypeId__: PaymentProcessedEvent
```

### Топіки та їх налаштування

- `10.orders.created` → партиції: 3, retention: 7 днів, producer: order-service.
- `10.orders.cancelled` → партиції: 1, retention: 7 днів, producer: order-service.
- `10.payments.processed` → партиції: 3, retention: 7 днів, producer: payment-service.
- `10.orders.created-retry-0`, `10.orders.created-retry-1`, `10.orders.created-dlt` → auto-created.

## Ключові концепції цієї гілки

### Проблема з FQCN у `__TypeId__`

Без `spring.json.type.mapping` producer записує повне ім'я класу:

```
__TypeId__: com.kafkalab.order.model.OrderCreatedEvent
```

Consumer в іншому сервісі (наприклад `payment-service`) отримує цей header
і намагається знайти клас `com.kafkalab.order.model.OrderCreatedEvent` у своєму classpath.
Клас не знайдено → `ClassNotFoundException` → помилка десеріалізації.

### Рішення: spring.json.type.mapping

Producer записує alias замість FQCN:

```yaml
# order-service/application.yml (producer)
spring.json.type.mapping: "OrderCreatedEvent:com.kafkalab.order.model.OrderCreatedEvent"
# Header: __TypeId__: OrderCreatedEvent  ← alias, не FQCN
```

Consumer у кожному сервісі має власний маппінг alias → локальний клас:

```yaml
# payment-service/application.yml (consumer)
spring.json.type.mapping: "OrderCreatedEvent:com.kafkalab.payment.model.OrderCreatedEvent"
# "OrderCreatedEvent" → локальний клас payment-service

# notification-service/application.yml (consumer)
spring.json.type.mapping: "OrderCreatedEvent:com.kafkalab.notification.model.OrderCreatedEvent,
  OrderCancelledEvent:com.kafkalab.notification.model.OrderCancelledEvent,
  PaymentProcessedEvent:com.kafkalab.notification.model.PaymentProcessedEvent"
```

> Кожен сервіс має власну копію класу з тим самим ім'ям але в своєму пакеті.
> Вони не залежать один від одного (loose coupling).

### spring.json.trusted.packages

Без `trusted.packages` десеріалізатор відхилить будь-який клас з міркувань безпеки:

```yaml
spring:
  kafka:
    consumer:
      properties:
        spring.json.trusted.packages: "*"  # дозволяємо всі (для dev/test)
        # або:
        spring.json.trusted.packages: "com.kafkalab.payment.model"  # тільки цей пакет
```

В production краще вказувати конкретні пакети, а не `"*"`.

### Event versioning

```kotlin
// OrderCreatedEvent.kt — branch10
data class OrderCreatedEvent(
    val eventVersion: String = "1.0",     // ← NEW: версія схеми
    val orderId: String = UUID.randomUUID().toString(),
    val userId: String = "",
    val items: List<OrderItem> = emptyList(),  // ← NEW: вкладений список
    val totalAmount: Double = 0.0,
    val timestamp: String = LocalDateTime.now().toString()
)

// OrderItem.kt — NEW
data class OrderItem(
    val productId: String = UUID.randomUUID().toString(),
    val productName: String = "",
    val quantity: Int = 1,
    val unitPrice: Double = 0.0
)
```

Поле `eventVersion` дозволяє consumer-у визначити версію схеми і обробити відповідно:

```kotlin
if (event.eventVersion == "2.0") {
    // нова логіка
} else {
    // backward compatible логіка
}
```

### Nested objects

Jackson серіалізує `List<OrderItem>` як JSON array — окремий `type.mapping` не потрібен:

```json
{
  "eventVersion": "1.0",
  "orderId": "uuid",
  "userId": "alice",
  "items": [
    { "productId": "uuid", "productName": "Laptop", "quantity": 1, "unitPrice": 1500.0 }
  ],
  "totalAmount": 1500.0
}
```

### DLT → Business Event (DECLINED)

У branch09 `@DltHandler` лише логував.
Тепер він публікує `PaymentProcessedEvent(status=DECLINED)` у `10.payments.processed`:

```kotlin
// OrderPaymentListener.kt
@DltHandler
fun handleDlt(record: ConsumerRecord<String, OrderCreatedEvent>) {
    val event = record.value()
    val declined = PaymentProcessedEvent(
        orderId = event.orderId,
        userId = event.userId,
        amount = event.totalAmount,
        status = PaymentStatus.DECLINED,
        paymentId = "DECLINED-${UUID.randomUUID()}"
    )
    kafkaTemplate.send("10.payments.processed", event.userId, declined)
    log.error("[DLT] Order {} → published DECLINED event", event.orderId)
}
```

`notification-service` отримує `DECLINED` і логує відповідно:
`"⚠ Payment DECLINED for order {orderId} — user {userId} notified"`.

## Як запустити

```bash
docker compose -f docker-compose-10.yml up --build
```

Перевірити 5 контейнерів:

```bash
docker compose -f docker-compose-10.yml ps
# kafka, kafka-ui, order-service-b10, payment-service-b10, notification-service-b10
```

## Як протестувати

### 1. Створити замовлення — перевірити eventVersion і items

```bash
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"alice","product":"Laptop","quantity":1,"totalAmount":1500.0}'
```

Очікувана відповідь (тепер включає `eventVersion` і `items`):

```json
{
  "eventVersion": "1.0",
  "orderId": "uuid",
  "userId": "alice",
  "items": [{ "productName": "Laptop", "quantity": 1, "unitPrice": 1500.0 }],
  "totalAmount": 1500.0
}
```

### 2. Перевірити payment-service і notification-service

```bash
curl http://localhost:8083/api/payments/stats
# {"processed":1,"dlt":0,"failNextN":0}

curl http://localhost:8082/api/notifications/count
# {"orders_created":0,"orders_cancelled":0,"payments_processed":1,"total":1}
```

### 3. Симулювати збій платежу → DLT → DECLINED

```bash
# Запланувати 3 збої → повідомлення пройде retry-0 → retry-1 → DLT → DECLINED
curl -s -X POST http://localhost:8083/api/payments/simulate-failure/3

curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"bob","product":"Headphones","quantity":1,"totalAmount":200.0}'

# Після ~7с перевірити:
curl http://localhost:8083/api/payments/stats
# {"processed":0,"dlt":1}

curl http://localhost:8082/api/notifications/count
# payments_processed: 1 (DECLINED notification)
```

**Windows (PowerShell):**

```powershell
Invoke-RestMethod -Method POST -Uri http://localhost:8083/api/payments/simulate-failure/3
Invoke-RestMethod -Method POST -Uri http://localhost:8081/api/orders `
  -ContentType "application/json" `
  -Body '{"userId":"bob","product":"Headphones","quantity":1,"totalAmount":200.0}'
```

### 4. Перевірити `__TypeId__` header у Kafka UI

Відкрити: `http://localhost:8080`

- `Topics` → `10.orders.created` → будь-яке повідомлення → вкладка **Headers**:
  - `__TypeId__: OrderCreatedEvent` ← alias
- `Topics` → `10.payments.processed` → Headers:
  - `__TypeId__: PaymentProcessedEvent`

## Як це працює всередині

### Producer — order-service

```yaml
# order-service/application.yml — branch10
spring:
  kafka:
    producer:
      properties:
        spring.json.add.type.headers: true          # ← NEW: додає __TypeId__
        spring.json.type.mapping: "OrderCreatedEvent:com.kafkalab.order.model.OrderCreatedEvent,
          OrderCancelledEvent:com.kafkalab.order.model.OrderCancelledEvent"
```

```kotlin
// OrderCreatedEvent.kt — branch10 (нові поля)
data class OrderCreatedEvent(
    val eventVersion: String = "1.0",
    val orderId: String = UUID.randomUUID().toString(),
    val userId: String = "",
    val items: List<OrderItem> = emptyList(),   // ← NEW
    val totalAmount: Double = 0.0,
    val timestamp: String = LocalDateTime.now().toString()
)
```

### Consumer — payment-service

```yaml
# payment-service/application.yml — consumer
spring.json.type.mapping: "OrderCreatedEvent:com.kafkalab.payment.model.OrderCreatedEvent"
spring.json.trusted.packages: "*"
```

### Consumer — notification-service (слухає 3 топіки)

```kotlin
// NotificationListener.kt — branch10
@KafkaListener(topics = ["10.orders.created"], groupId = "notification-service-group")
fun handleOrderCreated(record: ConsumerRecord<String, OrderCreatedEvent>) {
    log.info("[NOTIFICATION] Order created: orderId={}", record.value().orderId)
}

@KafkaListener(topics = ["10.payments.processed"], groupId = "notification-service-group")
fun handlePaymentProcessed(record: ConsumerRecord<String, PaymentProcessedEvent>) {
    val event = record.value()
    if (event.status == "DECLINED") {
        log.warn("[NOTIFICATION] ⚠ Payment DECLINED for order {}", event.orderId)
    } else {
        log.info("[NOTIFICATION] ✓ Payment APPROVED for order {}", event.orderId)
    }
    paymentsProcessedCount.incrementAndGet()
}
```

## Структура проєкту (зміни відносно branch09)

```
kafka-laboratory/
├── docker-compose-10.yml                          ← 5 контейнерів, топіки 10.*
├── branch10_json_serialization/
│   ├── order-service/
│   │   └── model/
│   │       ├── OrderCreatedEvent.kt               ← +eventVersion, +items: List<OrderItem>
│   │       └── OrderItem.kt                       ← NEW
│   ├── payment-service/
│   │   ├── model/
│   │   │   ├── PaymentProcessedEvent.kt           ← NEW
│   │   │   └── PaymentStatus.kt                   ← NEW enum APPROVED/DECLINED
│   │   └── listener/OrderPaymentListener.kt       ← @DltHandler тепер публікує DECLINED
│   └── notification-service/                      ← NEW сервіс
│       └── listener/NotificationListener.kt       ← слухає 3 топіки
│       └── src/main/resources/
│           └── application.yml                    ← 3 type.mapping записи
└── README10.md
```

## Що далі — branch11

- **Apache Avro** — бінарна серіалізація, `.avsc` схеми, генерація Java-класів.
- **Confluent Schema Registry** — центральне сховище схем з версіонуванням.
- **Backward / Forward / Full compatibility** — режими перевірки сумісності схем.
- **Розмір повідомлення** — Avro ~80 байт vs JSON ~300 байт для тих самих даних.
- **Schema evolution** — безпечне додавання полів з `default` значеннями.

---
---

## Слайди для презентації (11 слайдів)

**Слайд 1: Branch 10 — JSON Serialization**
- Apache Kafka for Certification & Production
- type.mapping, type headers, event versioning, pipeline

**Слайд 2: Agenda**
1. Проблема FQCN у __TypeId__ header
2. spring.json.type.mapping — alias замість FQCN
3. spring.json.trusted.packages — whitelist
4. spring.json.add.type.headers
5. Event versioning — eventVersion
6. Nested objects — List<OrderItem>
7. Multi-topic pipeline
8. DLT → business event (DECLINED)
9. Key takeaways & CCDAK

**Слайд 3: Проблема — ClassNotFoundException**
- JsonSerializer записує FQCN у __TypeId__: `com.kafkalab.order.model.OrderCreatedEvent`
- Consumer в іншому сервісі не знає цього класу → ClassNotFoundException
- Рішення потрібне: loose coupling між producer і consumer

**Слайд 4: spring.json.type.mapping**
- Producer: alias → FQCN (записує alias у __TypeId__)
- Consumer: alias → локальний FQCN (десеріалізує у свій клас)
- Кожен сервіс має власну копію класу в своєму пакеті
- Зміна namespace у producer не ламає consumers

**Слайд 5: spring.json.trusted.packages**
- Захист від довільної десеріалізації (Java deserialization attack)
- `"*"` — дозволяємо всі (dev/test)
- Production: конкретні пакети `"com.kafkalab.payment.model"`
- Без trusted.packages — ClassNotTrustedException

**Слайд 6: Event Versioning**
- `eventVersion: String = "1.0"` — версія схеми у кожній події
- Дозволяє consumer розрізняти v1.0 і v2.0 повідомлення
- Backward compatible: нове поле з default value
- Необхідно для rolling deployment без downtime

**Слайд 7: Nested Objects**
- `List<OrderItem>` серіалізується Jackson автоматично
- Окремий type.mapping для OrderItem не потрібен
- Default value `emptyList()` — backward compatible
- JSON array у payload: `"items":[{...},{...}]`

**Слайд 8: Multi-topic Pipeline**
- order → 10.orders.created → payment → 10.payments.processed → notification
- Кожен сервіс producer і consumer одночасно (крім order і notification)
- notification слухає 3 топіки: orders.created, orders.cancelled, payments.processed
- Єдина type.mapping у notification для 3 типів подій

**Слайд 9: DLT → Business Event**
- Branch09: @DltHandler тільки логував
- Branch10: @DltHandler публікує PaymentProcessedEvent(DECLINED)
- notification-service отримує DECLINED і сповіщає користувача
- Бізнес-логіка завершена навіть при failure — через DLT

**Слайд 10: Key Takeaways**
- `spring.json.type.mapping` — обов'язковий для cross-service Kafka у Java/Kotlin
- `trusted.packages` — security best practice для production
- Event versioning — необхідний для backward compatibility
- DLT → business event: кращий DLT pattern ніж просте логування
- Кожен сервіс має власну копію event-класів (loose coupling)

**Слайд 11: What's Next — Branch 11: Schema Registry & Avro**
- Apache Avro — бінарний формат, строга типізація
- Schema Registry — центральне сховище схем
- Backward/Forward/Full compatibility
- Менший розмір повідомлення: ~80 байт vs ~300 байт

## Текст для презентації (скрипт)

**Слайд 1:**
Десята гілка — JSON Serialization.
Тут ми вирішуємо реальну проблему що виникає у кожному мікросервісному Kafka-проекті:
як безпечно передавати typed-об'єкти між сервісами де у кожного свій classpath?

**Слайд 2:**
Розглянемо дев'ять тем від проблеми ClassNotFoundException до повного тристороннього pipeline.
Ключовий інсайт: серіалізація — це контракт між producer і consumer.

**Слайд 3:**
Без type mapping Jackson записує повне ім'я класу у header __TypeId__.
Коли payment-service читає це повідомлення — він не має com.kafkalab.order.model.OrderCreatedEvent.
ClassNotFoundException зупиняє consumer.
Це типова помилка при першому налаштуванні cross-service Kafka.

**Слайд 4:**
spring.json.type.mapping вирішує проблему через alias.
Producer записує короткий alias "OrderCreatedEvent" замість FQCN.
Кожен consumer-сервіс має власний маппінг alias → свій локальний клас.
Сервіси не залежать від пакету один одного — loose coupling.

**Слайд 5:**
trusted.packages — security механізм.
Без нього десеріалізатор може відмовити навіть legitimate класам.
"*" зручно для розробки але небезпечно у production.
У production — конкретний пакет або набір пакетів.

**Слайд 6:**
eventVersion — простий але ефективний механізм версіонування.
Producer ставить "1.0" у кожну подію.
Коли схема зміниться — producer почне ставити "2.0".
Consumer перевіряє версію і обробляє відповідно — backward compatible rolling deployment.

**Слайд 7:**
Вкладені об'єкти в Kafka через JSON — просто і зручно.
List<OrderItem> серіалізується Jackson автоматично без додаткового налаштування.
Default emptyList() гарантує backward compatibility — старі consumers не ломаються.
JSON array у payload читається природньо.

**Слайд 8:**
Тристоронній pipeline — реалістичніша архітектура.
order-service публікує замовлення, payment-service обробляє і публікує результат.
notification-service слухає обидва типи подій і сповіщає користувача.
Кожен сервіс незалежний — можна деплоїти окремо.

**Слайд 9:**
DLT pattern стає потужнішим коли @DltHandler публікує бізнес-подію.
Замість тихого логування — payment-service публікує PaymentProcessedEvent(DECLINED).
notification-service отримує і сповіщає користувача про відхилений платіж.
Бізнес-логіка завершується правильно навіть при технічній помилці.

**Слайд 10:**
Головні висновки: spring.json.type.mapping обов'язковий для будь-якого multi-service Kafka.
trusted.packages — завжди вказувати конкретні пакети в production.
Event versioning — добра практика з першого дня, дешевша ніж міграція пізніше.
DLT як бізнес-event а не просте логування — еволюція error handling.

**Слайд 11:**
Наступна гілка — Schema Registry і Avro.
Avro вирішує ті самі проблеми що і JSON type.mapping але ефективніше.
Schema Registry — центральне сховище схем з перевіркою сумісності.
Розмір повідомлення зменшується з 300 до 80 байт.

## Тестові питання (до 10 питань)

**Питання 1:**
Consumer отримав повідомлення з `__TypeId__: com.kafkalab.order.model.OrderCreatedEvent`.
У consumer-сервісі немає цього класу. Що відбудеться?

A) Spring Kafka автоматично конвертує тип
B) ClassNotFoundException при десеріалізації → consumer error
C) Повідомлення буде пропущено без помилки
D) Consumer запросить клас від producer через мережу

**Відповідь:** B —
`__TypeId__` header вказує JsonDeserializer який клас використати.
Якщо клас не знайдено у classpath — `ClassNotFoundException`.

---

**Питання 2:**
Яка мета `spring.json.type.mapping` у producer?

A) Перетворити JSON у бінарний формат
B) Записати alias замість FQCN у `__TypeId__` header
C) Вимкнути `__TypeId__` header
D) Додати compression до повідомлення

**Відповідь:** B —
`spring.json.type.mapping: "OrderCreatedEvent:com.kafkalab.order.model.OrderCreatedEvent"` →
producer записує `__TypeId__: OrderCreatedEvent` (alias) замість FQCN.

---

**Питання 3:**
`spring.json.trusted.packages: "*"` — яку загрозу це несе у production?

A) Всі повідомлення будуть відхилені
B) Зловмисник може підробити `__TypeId__` і змусити десеріалізувати шкідливий клас
C) Throughput знизиться через додаткові перевірки
D) Ніякої загрози — це лише конфігурація

**Відповідь:** B —
Без whitelist довільний `__TypeId__` у header може ініціювати десеріалізацію класу
з небажаною поведінкою (Java deserialization gadget chain).
У production: вказувати конкретні пакети.

---

**Питання 4:**
`OrderCreatedEvent` у `order-service` має namespace `com.kafkalab.order.model`.
У `payment-service` той самий клас має namespace `com.kafkalab.payment.model`.
Як вони комунікують через Kafka?

A) Необхідно мати спільну бібліотеку з єдиним namespace
B) `spring.json.type.mapping` маппить alias `OrderCreatedEvent` до локального класу в кожному сервісі
C) Spring Kafka автоматично знаходить правильний клас
D) Потрібен Schema Registry для маппінгу

**Відповідь:** B —
Producer: alias → `com.kafkalab.order.model.OrderCreatedEvent`.
Consumer (payment): alias → `com.kafkalab.payment.model.OrderCreatedEvent`.
Кожен сервіс має свою копію класу — loose coupling.

---

**Питання 5:**
`OrderCreatedEvent` має поле `items: List<OrderItem>` з default `emptyList()`.
Старий consumer (branch09) що не знає про `items` отримує нове повідомлення.
Що відбудеться?

A) ClassNotFoundException — `OrderItem` не знайдено
B) Jackson ігнорує невідомі поля за замовчуванням → десеріалізація успішна
C) Помилка: `items` є обов'язковим полем
D) Consumer отримає NullPointerException

**Відповідь:** B —
Jackson за замовчуванням ігнорує невідомі поля (`DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES=false`).
Старий consumer отримає знайомі поля, `items` буде проігнорований.

---

**Питання 6:**
Яку роль відіграє `eventVersion: String = "1.0"` у event-моделі?

A) Kafka metadata — автоматично обробляється broker
B) Дозволяє consumer розрізняти версії схеми і обробляти backward compatible
C) Обов'язкове поле для Spring Kafka JsonDeserializer
D) Використовується для routing до різних партицій

**Відповідь:** B —
`eventVersion` — бізнес-поле у payload.
Дозволяє consumer перевірити `if (event.eventVersion == "2.0")` і застосувати нову логіку.
При rolling deployment: старі consumers обробляють "1.0", нові — і "1.0" і "2.0".

---

**Питання 7:**
`notification-service` слухає `10.payments.processed`.
`payment-service` публікує `PaymentProcessedEvent` з alias `PaymentProcessedEvent`.
Що потрібно у `notification-service/application.yml`?

A) Нічого — Spring Kafka автоматично десеріалізує
B) `spring.json.type.mapping: "PaymentProcessedEvent:com.kafkalab.notification.model.PaymentProcessedEvent"`
C) `spring.json.type.mapping: "PaymentProcessedEvent:com.kafkalab.payment.model.PaymentProcessedEvent"`
D) `spring.json.trusted.packages: "com.kafkalab.payment.model"`

**Відповідь:** B —
notification-service має власну копію `PaymentProcessedEvent` у своєму пакеті.
type.mapping маппить alias → локальний клас notification-сервісу.

---

**Питання 8:**
Чому `@DltHandler` у branch10 публікує `PaymentProcessedEvent(DECLINED)` замість простого логування?

A) Spring Kafka вимагає публікації після DLT
B) Щоб notification-service міг сповістити користувача про відмову платежу
C) Для зменшення lag у DLT топіку
D) Для сумісності з Schema Registry

**Відповідь:** B —
Просте логування ховає відмову всередині сервісу.
Публікація DECLINED → notification-service отримує подію → сповіщає користувача.
Бізнес-процес завершується коректно навіть при технічній помилці.

---

**Питання 9:**
`spring.json.add.type.headers: true` налаштовано в producer.
`spring.json.add.type.headers: false` налаштовано в іншому producer для того ж топіку.
Consumer бачить обидва типи повідомлень. Що відбудеться?

A) Consumer завжди успішно десеріалізує
B) Повідомлення без `__TypeId__` → помилка десеріалізації якщо тип не вказано явно
C) Spring Kafka автоматично визначить тип по JSON-структурі
D) Повідомлення без header будуть відправлені до DLT

**Відповідь:** B —
Без `__TypeId__` header десеріалізатор не знає в який клас перетворити JSON.
Потрібно або вказати `ValueType` в конфігурації consumer, або зберегти `add.type.headers: true`.

---

**Питання 10:**
У `10.payments.processed` публікують і `APPROVED` і `DECLINED` PaymentProcessedEvent.
`notification-service` обробляє обидва. Яка конфігурація потрібна?

A) Два окремих `@KafkaListener` для APPROVED і DECLINED
B) Один `@KafkaListener` для `10.payments.processed` + перевірка `event.status` в коді
C) Два окремих топіки: `10.payments.approved` і `10.payments.declined`
D) Фільтр на рівні `@KafkaListener(topics=["..."], condition="...")` 

**Відповідь:** B —
Один listener читає всі повідомлення з топіку.
Розгалуження логіки відбувається всередині listener: `if (event.status == "DECLINED") { ... }`.
Це найпростіший і найефективніший підхід.