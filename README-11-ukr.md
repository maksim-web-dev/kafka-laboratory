# Branch 11 — Schema Registry & Avro

## `branch11_schema_registry` — що вивчаємо

- **Apache Avro** — бінарна серіалізація з `.avsc` файлами схем і генерацією Java-класів.
- **Confluent Schema Registry** — HTTP-сервіс для зберігання, версіонування і перевірки схем.
- **`KafkaAvroSerializer` / `KafkaAvroDeserializer`** — Confluent serializer для Avro.
- **`specific.avro.reader: true`** — десеріалізатор використовує конкретний generated клас.
- **Gradle Avro Plugin** — `com.github.davidmc24.gradle.plugin.avro` генерує Java-класи з `.avsc`.
- **Schema evolution** — Backward / Forward / Full compatibility при зміні схеми.
- **Розмір повідомлення** — Avro ~80 байт vs JSON ~300 байт для однакових даних.
- **Schema ID в payload** — кожне Avro-повідомлення містить 5-байтний prefix: `0x00` + 4 байти schema ID.
- **Schema Registry REST API** — `/subjects`, `/versions/latest`, `/compatibility`.
- **`OrderItem` як вкладена Avro-схема** — `"type": "array", "items": "com.kafkalab.avro.OrderItem"`.

## Що змінилося порівняно з branch10

- `JsonSerializer` / `JsonDeserializer` → **`KafkaAvroSerializer` / `KafkaAvroDeserializer`**.
- Замість data class Kotlin → **generated Java-класи** з Avro Gradle Plugin (`.avsc` → `.java`).
- `spring.json.type.mapping` → **`specific.avro.reader: true`** + schema ID у payload.
- `spring.json.add.type.headers` → **не потрібен** (schema ID вбудований у payload).
- Доданий новий контейнер **`schema-registry-b11`** на порту **8090**.
- `schema.registry.url` налаштовано у всіх трьох сервісах.
- Схеми у `src/main/avro/` — `.avsc` файли для кожного event-типу.
- Назви топіків змінено на `11.orders.*`, `11.payments.*`.

## Архітектура

```
┌──────────────────────────────────────────────────────────────────┐
│                     Schema Registry :8090                        │
│  subjects:                                                       │
│    11.orders.created-value   → схема OrderCreatedEvent v1        │
│    11.orders.cancelled-value → схема OrderCancelledEvent v1      │
│    11.payments.processed-value → схема PaymentProcessedEvent v1  │
└──────────────┬───────────────────────────────────────────────────┘
               │  реєстрація при першій публікації
               │  отримання schema ID при кожній публікації
┌──────────────┴──────────────────────────────────────────────────┐
│  order-service :8081                                             │
│  KafkaAvroSerializer → реєструє схему → отримує schema-id=1      │
│  payload: [0x00][0x00 0x00 0x00 0x01][avro binary data]         │
└────────────────────────┬────────────────────────────────────────┘
                         │
          ┌──────────────┴──────────────────┐
          │                                  │
   11.orders.created                  11.orders.cancelled
          │                                  │
┌─────────▼──────────────┐        ┌──────────▼───────────────────┐
│  payment-service :8083  │        │  notification-service :8082   │
│  KafkaAvroDeserializer  │        │  KafkaAvroDeserializer        │
│  specific.avro.reader   │        │  specific.avro.reader         │
└─────────┬───────────────┘        └──────────────────────────────┘
          │
   11.payments.processed
          │
┌─────────▼──────────────┐
│  notification-service   │
│  KafkaAvroDeserializer  │
└────────────────────────┘
```

### Топіки та їх налаштування

- `11.orders.created` → партиції: 3, retention: 7 днів, Avro schema: OrderCreatedEvent v1.
- `11.orders.cancelled` → партиції: 1, retention: 7 днів, Avro schema: OrderCancelledEvent v1.
- `11.payments.processed` → партиції: 3, retention: 7 днів, Avro schema: PaymentProcessedEvent v1.

## Ключові концепції цієї гілки

### Avro vs JSON — порівняння

```
JSON повідомлення (OrderCreatedEvent):
{
  "eventVersion": "1.0",
  "orderId": "550e8400-e29b-41d4-a716-446655440000",
  "userId": "alice",
  "items": [{"productName":"Laptop","quantity":1,"unitPrice":1500.0}],
  "totalAmount": 1500.0,
  "timestamp": "2024-01-15T10:30:00"
}
→ ~280 байт (назви полів у кожному повідомленні)

Avro повідомлення (той самий OrderCreatedEvent):
[0x00][schema-id: 4 bytes][avro binary]
→ ~80 байт (назви полів тільки в схемі, payload — чисті байти)
```

- Avro payload: 5-байтний prefix + бінарні дані без назв полів.
- Schema Registry: `schema-id=1` → fetch схему → десеріалізувати.
- ~3.5x менший розмір → менше мережевого трафіку, менше storage.

### .avsc схема і генерація коду

```json
// src/main/avro/OrderCreatedEvent.avsc
{
  "type": "record",
  "name": "OrderCreatedEvent",
  "namespace": "com.kafkalab.avro",
  "fields": [
    {"name": "eventVersion", "type": "string", "default": "1.0"},
    {"name": "orderId",      "type": "string", "default": ""},
    {"name": "userId",       "type": "string", "default": ""},
    {"name": "items",        "type": {"type": "array", "items": "com.kafkalab.avro.OrderItem"}, "default": []},
    {"name": "totalAmount",  "type": "double",  "default": 0.0},
    {"name": "timestamp",    "type": "string",  "default": ""}
  ]
}
```

Gradle Avro Plugin (`./gradlew generateAvroJava`) генерує:

```java
// build/generated-main-avro-java/com/kafkalab/avro/OrderCreatedEvent.java
// (автоматично, не редагуємо вручну)
public class OrderCreatedEvent extends SpecificRecordBase implements SpecificRecord {
    private String eventVersion;
    private String orderId;
    // ...
    public static OrderCreatedEvent.Builder newBuilder() { ... }
}
```

### Schema Registry — реєстрація і отримання схеми

```
Producer (перша публікація):
  1. KafkaAvroSerializer → HTTP POST /subjects/11.orders.created-value/versions
  2. Schema Registry → збереження схеми → повертає schema-id=1
  3. Повідомлення: [0x00][0x00 0x00 0x00 0x01][binary data]

Producer (наступні публікації):
  1. KafkaAvroSerializer → шукає schema-id у локальному кеші → cache hit!
  2. Повідомлення: [0x00][0x00 0x00 0x00 0x01][binary data]
  (HTTP запит до Schema Registry не потрібен)

Consumer (десеріалізація):
  1. Reads prefix → schema-id=1
  2. KafkaAvroDeserializer → HTTP GET /schemas/ids/1
  3. Schema cached → deserialize binary data → OrderCreatedEvent object
```

### Schema Evolution — три режими

**Backward compatible (додати поле з default ✅):**

```json
{"name": "discountCode", "type": "string", "default": ""}
```

- Старі consumers читають нові повідомлення: нове поле = default значення.
- Schema Registry приймає нову версію.

**Incompatible (видалити поле без default ❌):**

```bash
curl -X POST http://localhost:8090/compatibility/subjects/11.orders.created-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "... без orderId ..."}'
# {"is_compatible":false}
```

Schema Registry відхиляє реєстрацію — producer отримає `SchemaRegistryException`.

**Режими сумісності:**

```
BACKWARD  — нова схема читає старі повідомлення (додавати поля з default)
FORWARD   — стара схема читає нові повідомлення (видаляти поля з default)
FULL      — і нова читає старі, і стара читає нові (найбільш обмежений)
NONE      — без перевірок (не рекомендується для production)
```

## Як запустити

```bash
docker compose -f docker-compose-11.yml up --build
```

Перевірити 6 контейнерів:

```bash
docker compose -f docker-compose-11.yml ps
# kafka, schema-registry-b11, kafka-ui
# order-service-b11, payment-service-b11, notification-service-b11
```

## Як протестувати

### 1. Перевірити що Schema Registry запустився

```bash
curl http://localhost:8090/subjects
# [] (порожній на початку)
```

### 2. Створити замовлення — схема реєструється автоматично

```bash
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-42","itemCount":3}'
```

### 3. Перевірити зареєстровані схеми

```bash
# Всі subjects
curl http://localhost:8090/subjects
# ["11.orders.created-value","11.orders.cancelled-value","11.payments.processed-value"]

# Деталі схеми
curl http://localhost:8090/subjects/11.orders.created-value/versions/latest | python3 -m json.tool
```

**Windows (PowerShell):**

```powershell
Invoke-RestMethod http://localhost:8090/subjects
Invoke-RestMethod http://localhost:8090/subjects/11.orders.created-value/versions/latest
```

### 4. Симулювати збій платежу

```bash
curl -s -X POST "http://localhost:8083/api/payments/fail-next?count=1"

curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-42","itemCount":1}'
```

### 5. Перевірити backward compatible еволюцію схеми

```bash
# Додаємо нове поле discountCode з default "" — backward compatible
curl -s -X POST http://localhost:8090/compatibility/subjects/11.orders.created-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schema": "{\"type\":\"record\",\"name\":\"OrderCreatedEvent\",\"namespace\":\"com.kafkalab.avro\",\"fields\":[{\"name\":\"eventVersion\",\"type\":\"string\",\"default\":\"1.0\"},{\"name\":\"orderId\",\"type\":\"string\",\"default\":\"\"},{\"name\":\"userId\",\"type\":\"string\",\"default\":\"\"},{\"name\":\"items\",\"type\":{\"type\":\"array\",\"items\":\"com.kafkalab.avro.OrderItem\"},\"default\":[]},{\"name\":\"totalAmount\",\"type\":\"double\",\"default\":0.0},{\"name\":\"timestamp\",\"type\":\"string\",\"default\":\"\"},{\"name\":\"discountCode\",\"type\":\"string\",\"default\":\"\"}]}"
  }'
# {"is_compatible":true}
```

### 6. Kafka UI з Schema Registry

Відкрити: `http://localhost:8080`

- `Topics` → `11.orders.created` → повідомлення десеріалізуються у JSON view.
- `Schema Registry` → бачимо всі registered subjects і версії.

### 7. Переглянути schema-id у raw message

```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic 11.orders.created \
  --from-beginning \
  --max-messages 1 \
  --property print.headers=false
# Перші байти: 0x00 + 4-byte schema-id (binary, не людино-читабельні)
```

## Як це працює всередині

### Producer — order-service

```yaml
# order-service/application.yml — branch11
spring:
  kafka:
    bootstrap-servers: ${SPRING_KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    properties:
      schema.registry.url: ${SCHEMA_REGISTRY_URL:http://localhost:8090}
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: io.confluent.kafka.serializers.KafkaAvroSerializer
```

```kotlin
// OrderService.kt — використовує generated Avro клас
val event = OrderCreatedEvent.newBuilder()
    .setEventVersion("1.0")
    .setOrderId(UUID.randomUUID().toString())
    .setUserId(userId)
    .setItems(items)
    .setTotalAmount(totalAmount)
    .setTimestamp(LocalDateTime.now().toString())
    .build()

kafkaTemplate.send("11.orders.created", userId, event)
```

### Consumer — payment-service / notification-service

```yaml
# payment-service/application.yml — consumer
spring:
  kafka:
    properties:
      schema.registry.url: ${SCHEMA_REGISTRY_URL:http://localhost:8090}
    consumer:
      value-deserializer: io.confluent.kafka.serializers.KafkaAvroDeserializer
      properties:
        specific.avro.reader: true  # ← використовує конкретний generated клас
```

```kotlin
// OrderPaymentListener.kt
@KafkaListener(topics = ["11.orders.created"], groupId = "payment-service-group")
fun handleOrderCreated(record: ConsumerRecord<String, OrderCreatedEvent>) {
    val event = record.value()    // ← generated Avro клас, не Kotlin data class
    log.info("[PAYMENT] Order {} from {} → amount={}", event.orderId, event.userId, event.totalAmount)
}
```

## Структура проєкту (зміни відносно branch10)

```
kafka-laboratory/
├── docker-compose-11.yml                              ← +schema-registry-b11 :8090
├── branch11_schema_registry/
│   ├── order-service/
│   │   ├── src/main/avro/                             ← NEW: .avsc файли
│   │   │   ├── OrderCreatedEvent.avsc
│   │   │   ├── OrderCancelledEvent.avsc
│   │   │   └── OrderItem.avsc
│   │   └── build.gradle.kts                           ← +avro plugin, +confluent serializer
│   ├── payment-service/
│   │   ├── src/main/avro/
│   │   │   ├── OrderCreatedEvent.avsc                 ← копія схеми
│   │   │   └── PaymentProcessedEvent.avsc
│   │   └── src/main/resources/application.yml        ← KafkaAvroSerializer/Deserializer
│   └── notification-service/
│       ├── src/main/avro/
│       │   ├── OrderCreatedEvent.avsc
│       │   ├── OrderCancelledEvent.avsc
│       │   └── PaymentProcessedEvent.avsc
│       └── src/main/resources/application.yml        ← specific.avro.reader: true
└── README11.md
```

## Що далі — branch12

- **Idempotent Producer (глибше)** — producer-id, sequence number, exactly-once.
- **Kafka Transactions** — `transaction-id-prefix`, `beginTransaction` / `abortTransaction`.
- **`read_committed`** — consumer бачить тільки committed транзакції.
- **`executeInTransaction`** — Spring Kafka API для транзакційного відправлення.
- **Abort demo** — notification-service не отримує повідомлення з aborted транзакції.

---
---

## Слайди для презентації (12 слайдів)

**Слайд 1: Branch 11 — Schema Registry & Avro**
- Apache Kafka for Certification & Production
- Avro, Schema Registry, schema evolution

**Слайд 2: Agenda**
1. JSON vs Avro — розмір і контракт
2. .avsc схема і генерація коду
3. Schema Registry — реєстрація і кешування
4. schema-id у payload
5. specific.avro.reader
6. Schema evolution — Backward compatible
7. Schema evolution — Forward, Full, None
8. Incompatible зміна → SchemaRegistryException
9. Schema Registry REST API
10. Key takeaways & CCDAK

**Слайд 3: JSON vs Avro**
- JSON: ~300 байт, назви полів у кожному повідомленні, немає контракту
- Avro: ~80 байт, назви полів тільки в схемі, строга типізація
- Avro: 5-байтний prefix (magic + schema-id) перед binary payload
- Avro: Schema Registry відхиляє несумісну схему до публікації

**Слайд 4: .avsc Схема**
- JSON-файл що описує структуру запису
- `type: "record"`, поля з іменем і типом
- Default значення → backward compatible еволюція
- Gradle Avro Plugin генерує Java-класи: `./gradlew generateAvroJava`

**Слайд 5: Schema Registry — Як Працює**
- HTTP сервіс для зберігання і версіонування схем
- Перша публікація: producer POST схему → отримує schema-id
- Наступні публікації: schema-id з кешу → без HTTP запиту
- Десеріалізація: schema-id з payload → GET схему → deserialize

**Слайд 6: schema-id у Payload**
- Кожне Avro повідомлення: `[0x00][4 bytes schema-id][binary avro data]`
- Magic byte `0x00` — Confluent wire format marker
- Consumer завжди знає яку схему використати
- Немає потреби в `__TypeId__` header (на відміну від JSON)

**Слайд 7: specific.avro.reader**
- `specific.avro.reader: true` — десеріалізатор використовує generated клас
- `specific.avro.reader: false` — десеріалізація у `GenericRecord` (динамічний)
- `SpecificRecord` — статично typed, IntelliJ автодоповнення
- `GenericRecord` — динамічний, підходить для generic pipeline

**Слайд 8: Schema Evolution — Backward**
- Backward compatible: нова схема читає старі повідомлення
- Додати поле з `default` ← завжди backward compatible
- Видалити поле без `default` ← backward incompatible
- Змінити тип поля ← завжди incompatible

**Слайд 9: Режими Сумісності**
- BACKWARD: нова читає старі повідомлення (найпоширеніший)
- FORWARD: стара читає нові повідомлення
- FULL: і нова читає старі, і стара читає нові
- NONE: без перевірок (не для production)

**Слайд 10: Incompatible Change**
- Видалення обов'язкового поля → Schema Registry відхиляє
- `{"error_code": 409, "message": "Schema incompatible..."}`
- Producer не може опублікувати несумісну схему
- Це safety net перед deployment

**Слайд 11: Key Takeaways**
- Avro + Schema Registry — production стандарт для Confluent platform
- Schema evolution через `default` значення — backward compatible
- schema-id у payload = немає проблеми ClassNotFoundException
- Schema Registry — compile-time + runtime контракт між сервісами
- Avro ~3.5x менший за JSON — суттєво при великих обсягах

**Слайд 12: What's Next — Branch 12: Idempotent Producer & Transactions**
- Idempotent producer — producer-id, sequence numbers
- Kafka Transactions — atomically publish to multiple topics
- read_committed — consumer бачить тільки committed
- Exactly-once semantics (EOS)

## Текст для презентації (скрипт)

**Слайд 1:**
Одинадцята гілка — Schema Registry і Avro.
Після JSON serialization ми переходимо до більш потужного рішення.
Avro і Schema Registry вирішують проблеми контракту, розміру і сумісності між сервісами.

**Слайд 2:**
Розглянемо десять тем від порівняння форматів до REST API Schema Registry.
Головний інсайт: Schema Registry — це not just optimization, а architectural governance інструмент.

**Слайд 3:**
JSON зручний але неефективний: назви полів у кожному повідомленні займають місце.
Avro зберігає назви полів тільки в схемі в Schema Registry.
Кожне повідомлення — 5 байт prefix і чисті бінарні дані.
Результат: 3-4 рази менший розмір і строга типізація.

**Слайд 4:**
avsc — JSON-файл що описує структуру event.
Gradle Avro Plugin автоматично генерує Java-класи з цих файлів.
Default значення у полях — ключ до backward compatible еволюції.
Без default — поле обов'язкове і його видалення поломає сумісність.

**Слайд 5:**
Schema Registry працює як HTTP сервіс.
При першій публікації producer реєструє схему і отримує числовий schema-id.
При наступних публікаціях — schema-id береться з локального кешу.
При десеріалізації consumer читає schema-id з payload і fetches схему.

**Слайд 6:**
Формат Confluent Wire: magic byte нуль, чотири байти schema-id, потім Avro binary.
Цей prefix є в кожному повідомленні.
Consumer завжди знає який schema-id використати — немає проблеми класу типу.
Це принципово відрізняє від JSON де потрібен TypeId header.

**Слайд 7:**
specific.avro.reader вибирає mode десеріалізації.
Specific: used generated клас — безпечний, з compile-time перевірками.
Generic: повертає GenericRecord — flexible але runtime access через get().
Для більшості production use-cases specific — кращий вибір.

**Слайд 8:**
Backward compatibility — найважливіший режим.
Нова версія схеми повинна читати старі повідомлення що ще в топіку.
Правило: якщо додаєш поле — обов'язково вказуй default.
Без default — нові consumers не можуть прочитати старі повідомлення.

**Слайд 9:**
Чотири режими сумісності відповідають різним сценаріям.
BACKWARD для більшості: rolling upgrade consumers перед producers.
FORWARD для rolling upgrade producers перед consumers.
FULL для двонаправленої сумісності.
NONE тільки для розробки чи внутрішніх топіків.

**Слайд 10:**
Incompatible зміна блокується на рівні Schema Registry.
Producer не може навіть опублікувати повідомлення зі схемою що порушує контракт.
Це захищає від ситуації коли "working in dev breaks in prod".
Набагато краще ніж ClassNotFoundException в runtime.

**Слайд 11:**
Ключові висновки: Avro є production стандартом для Confluent Kafka.
Schema evolution через default values — обов'язкова практика.
Schema Registry як governance інструмент: зміни схеми вимагають дотримання контракту.
Менший розмір повідомлення суттєво зменшує costs при великих обсягах.

**Слайд 12:**
Наступна гілка — Idempotent Producer і Transactions.
Це кульмінація producer-теми: exactly-once semantics.
Транзакції дозволяють atomic publish до кількох топіків.
read_committed consumer бачить тільки committed повідомлення.

## Тестові питання (до 10 питань)

**Питання 1:**
Яке значення Schema Registry у Kafka екосистемі?

A) Кешування Kafka повідомлень для швидшого читання
B) Зберігання, версіонування і перевірка сумісності схем serialization
C) Управління consumer groups та їх offset
D) Балансування навантаження між broker-ами

**Відповідь:** B —
Schema Registry — HTTP-сервіс для централізованого управління схемами.
Producers реєструють схеми, consumers fetches їх.
Перевірка сумісності блокує breaking changes до deployment.

---

**Питання 2:**
Avro повідомлення починається з байтів `[0x00][0x00 0x00 0x00 0x01]`.
Що означають ці 5 байтів?

A) Magic byte (Confluent Wire Format) + 4-байтний schema-id=1
B) Avro magic number + record length
C) Schema version + timestamp
D) Compression codec + checksum

**Відповідь:** A —
`0x00` — Confluent magic byte (Wire Format marker).
Наступні 4 байти — schema-id (big-endian int32).
`0x00 0x00 0x00 0x01` = schema-id = 1.

---

**Питання 3:**
Розробник хоче додати нове поле `discountCode: String` до `OrderCreatedEvent`.
Яка умова забезпечить backward compatible зміну?

A) Вказати `"default": ""` у .avsc файлі
B) Вказати поле як `"type": "null"`
C) Видалити поле `timestamp` перед додаванням нового
D) Збільшити `eventVersion` у всіх існуючих consumers

**Відповідь:** A —
Backward compatible: нова схема читає старі повідомлення.
Старі повідомлення не мають `discountCode` → потрібен default для заповнення.
Без default → Schema Registry відхилить як incompatible.

---

**Питання 4:**
Consumer з `specific.avro.reader: true` отримує повідомлення.
Яку перевагу це дає порівняно з `specific.avro.reader: false`?

A) Менший розмір повідомлення
B) Статично typed клас з compile-time перевірками vs динамічний GenericRecord
C) Автоматична перевірка schema compatibility
D) Можливість читати повідомлення без Schema Registry

**Відповідь:** B —
`specific.avro.reader: true` → generated клас (SpecificRecord) зі статичними методами.
`specific.avro.reader: false` → GenericRecord → `record.get("fieldName")` в runtime.
Specific більш безпечний і зручний для типізованого коду.

---

**Питання 5:**
Яка різниця між BACKWARD і FORWARD compatibility у Schema Registry?

A) BACKWARD: нова схема читає старі повідомлення; FORWARD: стара читає нові
B) BACKWARD: стара схема читає нові повідомлення; FORWARD: нова читає старі
C) BACKWARD і FORWARD — різні назви одного концепту
D) BACKWARD для producer; FORWARD для consumer

**Відповідь:** A —
BACKWARD: можна upgrade consumers до того як upgrade producers (нова схема backward-compatible).
FORWARD: можна upgrade producers до того як upgrade consumers (стара схема читає нові повідомлення).

---

**Питання 6:**
Producer намагається опублікувати повідомлення зі схемою що порушує BACKWARD compatibility.
Що відбудеться?

A) Повідомлення буде опублікованим але з warning в логах
B) Schema Registry поверне помилку; producer не зможе зареєструвати схему
C) Kafka broker відхилить повідомлення
D) Consumer групи отримають alert

**Відповідь:** B —
Schema Registry перевіряє сумісність при реєстрації нової версії.
Incompatible схема → HTTP 409 Conflict → `SchemaRegistryException` у producer.
Повідомлення не буде опубліковано.

---

**Питання 7:**
Яка приблизна різниця у розмірі між JSON і Avro для типового OrderCreatedEvent (~8 полів)?

A) Avro вдвічі більший — більше метаданих
B) Приблизно однаковий
C) Avro приблизно у 3-4 рази менший
D) Avro менший тільки для великих об'єктів (>1KB)

**Відповідь:** C —
JSON ~300 байт (назви полів у кожному повідомленні).
Avro ~80 байт (5 байт prefix + бінарні дані без назв полів).
Різниця ~3.5x суттєво при мільйонах повідомлень.

---

**Питання 8:**
Яка мета Gradle Avro Plugin у проекті?

A) Автоматичне публікування схем у Schema Registry при збірці
B) Генерація Java-класів з `.avsc` файлів при `./gradlew build`
C) Перевірка сумісності між версіями Avro
D) Конвертація JSON повідомлень у Avro формат

**Відповідь:** B —
Avro Plugin читає `.avsc` файли у `src/main/avro/` і генерує Java-класи.
Ці класи потрапляють у `build/generated-main-avro-java/`.
Розробники не редагують generated класи — тільки `.avsc` схеми.

---

**Питання 9:**
Чому producer не повинен залежати від classpath consumer-а при Avro?

A) Avro не підтримує залежності між сервісами
B) schema-id у payload → consumer fetches схему з Registry → без classpath залежності
C) Confluent serializer автоматично конвертує типи
D) Avro схеми компілюються у shared JAR

**Відповідь:** B —
Producer записує schema-id (число) у payload.
Consumer fetches схему від Registry за цим ID.
Сервіси мають власні копії схем у своїх `.avsc` файлах — loose coupling.
Немає потреби у shared library між сервісами.

---

**Питання 10:**
Компанія хоче максимально строгу перевірку: і новий code читає старі повідомлення,
і старий code читає нові. Який режим сумісності обрати?

A) BACKWARD
B) FORWARD
C) FULL
D) NONE

**Відповідь:** C —
FULL = BACKWARD + FORWARD одночасно.
Найбільш обмежений режим — більше правил для розробників.
Підходить для критичних систем де будь-яке breaking change неприйнятне.