# Branch 12 — Idempotent Producer & Transactions

## `branch12_idempotent_producer` — що вивчаємо

- **Idempotent Producer** — `enable.idempotence=true` з producer-id і sequence numbers.
- **Exactly-Once Semantics (EOS)** — повідомлення або committed, або відхилено — без дублікатів і втрат.
- **`transactional.id`** / `transaction-id-prefix` — унікальний ідентифікатор транзакційного producer.
- **`kafkaTemplate.executeInTransaction`** — Spring Kafka API для atomic публікації.
- **`abortTransaction`** — відкочування: повідомлення фізично записано але марковано ABORT.
- **`read_committed`** — consumer бачить тільки committed повідомлення, ігнорує ABORT.
- **`read_uncommitted`** (default) — consumer бачить всі повідомлення включно з незакомміченими.
- **Control batch** — службовий запис у топіку з типом COMMIT або ABORT (видно у Kafka UI).
- **Transactional recovery** — Kafka відновлює незавершені транзакції при рестарті producer.
- **`abort-next` endpoint** — симуляція abort для демо exactly-once гарантій.

## Що змінилося порівняно з branch11

- `order-service` — `enable.idempotence=true` (ретельніша конфігурація без змін API).
- `payment-service` — додано `transaction-id-prefix: payment-tx-` → транзакційний producer.
- `payment-service` — `executeInTransaction` замість прямого `kafkaTemplate.send`.
- `notification-service` — `isolation-level: read-committed` → бачить тільки committed.
- Schema Registry і Avro залишаються (ті самі serializer/deserializer з branch11).
- Новий ендпоінт `POST /api/payments/abort-next?count=N` — запланувати N aborts.
- Контейнер schema-registry-b12 (порт 8090) — ідентично branch11.
- Назви топіків змінено на `12.orders.*`, `12.payments.*`.

## Архітектура

```
order-service-b12 :8081
  Idempotent Producer:
  enable.idempotence=true, acks=all, retries=MAX
          │
   12.orders.created
          │
          ▼
payment-service-b12 :8083
  Transactional Producer:
  transaction-id-prefix: payment-tx-

  executeInTransaction:
    ┌─────────────────────────────────────────────┐
    │ 1. BEGIN TRANSACTION                         │
    │ 2. send(12.payments.processed, payment)      │
    │ 3. if shouldAbort: throw RuntimeException    │
    │    → abortTransaction (ABORT control batch)  │
    │    else: commitTransaction (COMMIT ctrl batch)│
    └─────────────────────────────────────────────┘
          │
   12.payments.processed
          │
          ▼
notification-service-b12 :8082
  isolation-level: read-committed
  → бачить: COMMITTED повідомлення ✓
  → ігнорує: ABORTED повідомлення ✗
```

### Топіки та їх налаштування

- `12.orders.created` → партиції: 3, retention: 7 днів, Avro, idempotent producer.
- `12.orders.cancelled` → партиції: 1, retention: 7 днів.
- `12.payments.processed` → партиції: 3, retention: 7 днів,
  transactional producer, control batches видно у Kafka UI.

## Ключові концепції цієї гілки

### Idempotent Producer — дедублікація retry

Проблема без idempotence:

```
Producer: send(orderId=ABC, seq=5)
  → мережевий збій (повідомлення дійшло, ACK загублено)
  → timeout → Retry: send(orderId=ABC, seq=5)
  → Broker записує ВДРУГЕ → дублікат у топіку ❌
```

З `enable.idempotence=true`:

```
Broker: "вже маємо (producer-id=42, partition=1, seq=5)" → duplicate? skip ✓

Вимоги (взаємопов'язані):
  acks=all                                    (обов'язково)
  retries ≥ 1                                 (обов'язково)
  max.in.flight.requests.per.connection ≤ 5   (обов'язково)
```

### Kafka Transactions — atomic publish

Transactional producer гарантує що група повідомлень або **всі committed** або **всі aborted**:

```kotlin
// payment-service/OrderPaymentListener.kt
kafkaTemplate.executeInTransaction { kt ->
    val payment = PaymentProcessedEvent.newBuilder()
        .setStatus("APPROVED")
        // ...
        .build()

    kt.send("12.payments.processed", event.orderId.toString(), payment)

    if (shouldAbort) {
        throw RuntimeException("Simulated abort")
        // Spring Kafka: abortTransaction() → control batch ABORT
    }
    // Spring Kafka: commitTransaction() → control batch COMMIT
}
```

### Abort flow — що відбувається в топіку

```
12.payments.processed топік:

Offset 0: [payment for order-A]  ← COMMITTED (control batch COMMIT)
Offset 1: [payment for order-B]  ← ABORTED (control batch ABORT)
Offset 2: [COMMIT marker]
Offset 3: [ABORT marker]

read_uncommitted consumer: бачить offset 0, 1 (і control batches)
read_committed consumer:   бачить тільки offset 0 (ігнорує offset 1 + ABORT marker)
```

Control batches (COMMIT/ABORT markers) **фізично** записані у топік але `read_committed` їх пропускає.
Видно у Kafka UI як "transaction marker" або "control batch".

### `transaction-id-prefix` і відновлення

`transactional.id` (або prefix) унікально ідентифікує producer instance:

```yaml
# payment-service/application.yml
spring:
  kafka:
    producer:
      transaction-id-prefix: payment-tx-
      # Kafka генерує: payment-tx-0, payment-tx-1, ... per partition
```

При рестарті producer:

```
1. Producer реєструється з тим самим transactional.id (payment-tx-0)
2. Kafka перевіряє чи є незавершені транзакції для цього ID
3. Якщо так → автоматично abort (не дозволяємо zombie transaction)
4. Новий producer починає чисту транзакцію
```

### `read_committed` vs `read_uncommitted`

```yaml
# notification-service/application.yml — branch12
spring:
  kafka:
    consumer:
      isolation-level: read-committed     # ← NEW: тільки committed

# Default (без налаштування):
#   isolation-level: read-uncommitted     # бачить всі, включно з незакомміченими
```

- `read_committed`: consumer не читає повідомлення поки транзакція не закрита.
  При відкритій транзакції: consumer "зупиняється" перед першим uncommitted offset.
  LAG зростатиме якщо транзакція не закривається.
- `read_uncommitted`: читає все одразу — може отримати повідомлення з aborted транзакцій.

## Як запустити

```bash
docker compose -f docker-compose-12.yml up --build
```

Перевірити 6 контейнерів:

```bash
docker compose -f docker-compose-12.yml ps
# kafka, schema-registry-b12, kafka-ui
# order-service-b12, payment-service-b12, notification-service-b12
```

## Як протестувати

### 1. Нормальний flow — notification отримує PaymentProcessed

```bash
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-1"}'

# Логи notification-service:
# [NOTIFICATION] Payment APPROVED for order uuid → user user-1
```

### 2. Головний демо-сценарій: abort транзакції

```bash
# Крок 1: Нормальне замовлення (committed)
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-commit"}'

# Крок 2: Запланувати abort наступної транзакції
curl -s -X POST "http://localhost:8083/api/payments/abort-next?count=1"
# {"abortScheduled":1}

# Крок 3: Надіслати замовлення що буде aborted
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-abort"}'

# Логи payment-service:
# [PAYMENT-TX] Processing orderId=uuid-2 abort=true
# RuntimeException: Simulated abort → abortTransaction

# Логи notification-service:
# (тільки перше замовлення — друге ABORTED, read_committed ігнорує)
```

**Windows (PowerShell):**

```powershell
Invoke-RestMethod -Method POST -Uri http://localhost:8081/api/orders `
  -ContentType "application/json" -Body '{"userId":"user-commit"}'

Invoke-RestMethod -Method POST -Uri "http://localhost:8083/api/payments/abort-next?count=1"

Invoke-RestMethod -Method POST -Uri http://localhost:8081/api/orders `
  -ContentType "application/json" -Body '{"userId":"user-abort"}'
```

### 3. Перевірити control batches у Kafka UI

Відкрити: `http://localhost:8080`

- `Topics` → `12.payments.processed` → Messages:
  - Перше повідомлення: normal record (COMMITTED).
  - Друге: invisible для read_committed (ABORTED).
  - Control batches: COMMIT marker + ABORT marker (service records).

### 4. Перевірити isolation level через CLI

```bash
# read_committed — бачить тільки committed:
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic 12.payments.processed \
  --from-beginning \
  --isolation-level read_committed

# read_uncommitted — бачить все (включно з ABORTED binary):
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic 12.payments.processed \
  --from-beginning \
  --isolation-level read_uncommitted
```

### 5. Schema Registry схеми

```bash
curl http://localhost:8090/subjects
# ["12.orders.created-value","12.payments.processed-value","..."]
```

## Як це працює всередині

### Producer — order-service (ідемпотентний)

```yaml
# order-service/application.yml — branch12
spring:
  kafka:
    producer:
      acks: all
      retries: 2147483647
      properties:
        enable.idempotence: true
        max.in.flight.requests.per.connection: 5
```

### Producer — payment-service (транзакційний)

```yaml
# payment-service/application.yml — branch12
spring:
  kafka:
    producer:
      acks: all
      retries: 2147483647
      properties:
        enable.idempotence: true
      transaction-id-prefix: payment-tx-   # ← NEW: транзакційний режим
```

```kotlin
// OrderPaymentListener.kt — branch12
@KafkaListener(topics = ["12.orders.created"], groupId = "payment-service-group")
fun handleOrderCreated(record: ConsumerRecord<String, OrderCreatedEvent>) {
    val event = record.value()
    val shouldAbort = abortNextN.get() > 0

    kafkaTemplate.executeInTransaction { kt ->
        val payment = PaymentProcessedEvent.newBuilder()
            .setStatus("APPROVED")
            .setOrderId(event.orderId.toString())
            .setUserId(event.userId.toString())
            .setAmount(event.totalAmount)
            .build()

        kt.send("12.payments.processed", event.orderId.toString(), payment)

        if (shouldAbort) {
            abortNextN.decrementAndGet()
            throw RuntimeException("Simulated abort for orderId=${event.orderId}")
            // Spring Kafka: автоматично викликає abortTransaction()
        }
        // Spring Kafka: автоматично викликає commitTransaction()
    }
}
```

### Consumer — notification-service (read_committed)

```yaml
# notification-service/application.yml — branch12
spring:
  kafka:
    consumer:
      isolation-level: read-committed     # ← NEW
```

## Структура проєкту (зміни відносно branch11)

```
kafka-laboratory/
├── docker-compose-12.yml                              ← schema-registry-b12 :8090
├── branch12_idempotent_producer/
│   ├── order-service/
│   │   └── src/main/resources/application.yml        ← enable.idempotence (вже було в b07)
│   ├── payment-service/
│   │   └── src/main/resources/application.yml        ← +transaction-id-prefix: payment-tx-
│   │   └── src/main/kotlin/.../listener/
│   │       └── OrderPaymentListener.kt               ← executeInTransaction + abort logic
│   │   └── src/main/kotlin/.../controller/
│   │       └── PaymentController.kt                  ← +abort-next endpoint
│   └── notification-service/
│       └── src/main/resources/application.yml        ← +isolation-level: read-committed
└── README12.md
```

## Що далі — branch13

- **Saga Pattern** — розподілені транзакції через послідовність Kafka-подій.
- **Choreography Saga** — кожен сервіс відповідає на події і публікує наступну.
- **Compensation events** — `OrderCancelledEvent` при збої будь-якого кроку.
- **Saga state machine** — відстеження стану саги через кілька топіків.
- **Idempotency в Saga** — захист від дублікатів при at-least-once delivery.

---
---

## Слайди для презентації (12 слайдів)

**Слайд 1: Branch 12 — Idempotent Producer & Transactions**
- Apache Kafka for Certification & Production
- EOS, transactions, read_committed

**Слайд 2: Agenda**
1. Проблема дублікатів без idempotence
2. Idempotent producer — producer-id + sequence
3. Транзакції — навіщо потрібні
4. transaction-id-prefix і відновлення
5. executeInTransaction
6. commitTransaction vs abortTransaction
7. Control batches у топіку
8. read_committed vs read_uncommitted
9. Zombie fencing
10. Key takeaways & CCDAK

**Слайд 3: Проблема Дублікатів**
- acks=1, retries=3: timeout → retry → broker записує ДВІЧІ
- Платіж нараховується двічі — критична помилка
- Idempotence: producer-id + sequence-number → broker skip duplicate
- Вимоги: acks=all + retries>0 + max.in.flight≤5

**Слайд 4: Transactional Producer**
- Idempotence: захист від дублікатів при retry (одна публікація)
- Transactions: atomic publish до КІЛЬКОХ топіків
- transaction-id-prefix → Kafka генерує transactional.id per partition
- Відновлення при рестарті: abort незавершених транзакцій

**Слайд 5: executeInTransaction**
- Spring Kafka API для транзакційного відправлення
- beginTransaction() автоматично
- commitTransaction() або abortTransaction() автоматично
- RuntimeException всередині lambda → abortTransaction

**Слайд 6: Нормальний Flow (COMMIT)**
- order-service: publish OrderCreatedEvent (idempotent)
- payment-service: executeInTransaction { send PaymentProcessedEvent }
- commitTransaction → COMMIT control batch у топіку
- notification-service: бачить PaymentProcessedEvent ✓

**Слайд 7: Abort Flow**
- order-service: publish OrderCreatedEvent (idempotent)
- payment-service: executeInTransaction { send PaymentProcessedEvent → throw Exception }
- abortTransaction → ABORT control batch у топіку
- notification-service: НЕ бачить PaymentProcessedEvent (read_committed) ✗

**Слайд 8: Control Batches**
- COMMIT/ABORT markers фізично записані у топік
- Kafka UI показує їх як "transaction marker"
- read_committed: ігнорує ABORTED повідомлення і ABORT markers
- read_uncommitted: бачить все включно з binary ABORTED

**Слайд 9: read_committed vs read_uncommitted**
- read_uncommitted (default): max throughput, бачить uncommitted
- read_committed: чекає commit/abort, бачить тільки committed
- LAG може зростати якщо транзакція відкрита занадто довго
- Production: read_committed для критичних consumers

**Слайд 10: Zombie Fencing**
- Старий producer instance (zombie) може конкурувати з новим
- Kafka fences zombie: якщо новий producer з тим самим transactional.id
  зареєструвався → старий отримує ProducerFencedException
- Гарантує: тільки один активний producer per transactional.id
- Захист від split-brain у distributed environment

**Слайд 11: Key Takeaways**
- Idempotence + Transactions = Exactly-Once Semantics (EOS) у Kafka
- EOS = atоmic: або всі повідомлення committed, або жодного
- read_committed обов'язковий для EOS consumer
- transactional.id з prefix: автоматичний rollback при рестарті
- EOS має вищу latency — тільки для критичних потоків

**Слайд 12: What's Next — Branch 13: Saga Pattern**
- Distributed transactions через послідовність Kafka-подій
- Choreography Saga: кожен сервіс відповідає на попередню подію
- Compensation events: rollback через business events
- Idempotency в Saga: захист від дублікатів

## Текст для презентації (скрипт)

**Слайд 1:**
Дванадцята гілка — Idempotent Producer і Transactions.
Це кульмінація теми producer надійності.
Тут ми досягаємо exactly-once semantics: повідомлення або committed, або відкинуто — без дублікатів і втрат.

**Слайд 2:**
Розглянемо десять тем від проблеми дублікатів до zombie fencing.
Головний інсайт: EOS у Kafka — це не одне налаштування, а система з кількох компонентів.

**Слайд 3:**
Без idempotence: при timeout producer повторює відправку.
Якщо першу спробу broker отримав але не встиг підтвердити — другий запис стає дублікатом.
Для платежів це критично: клієнт платить двічі.
Idempotence вирішує це через producer-id і sequence number per partition.

**Слайд 4:**
Transactions — наступний рівень після idempotence.
Idempotence захищає від дублікатів при retry однієї публікації.
Transactions гарантують що кілька публікацій у різні топіки — атомарні.
transaction-id-prefix ідентифікує producer і дозволяє відновлення при рестарті.

**Слайд 5:**
executeInTransaction — Spring Kafka API що огортає логіку у транзакцію.
beginTransaction викликається автоматично.
Якщо все пройшло — commitTransaction.
Якщо RuntimeException — abortTransaction.
Просте і безпечне API для транзакційної логіки.

**Слайд 6:**
Нормальний flow: order-service публікує замовлення.
payment-service починає транзакцію, відправляє PaymentProcessedEvent, commits.
COMMIT control batch записується у топік.
notification-service з read_committed бачить подію і реагує.

**Слайд 7:**
Abort flow: те саме але payment-service кидає виключення.
Kafka автоматично викликає abortTransaction.
ABORT control batch записується у топік.
notification-service не бачить PaymentProcessedEvent — транзакція відкочена.

**Слайд 8:**
Control batches — службові записи у топіку.
COMMIT і ABORT markers фізично є у Kafka log файлі.
Вони потрібні щоб consumer знав коли транзакція завершилась.
Kafka UI показує їх як transaction markers — корисно для debugging.

**Слайд 9:**
read_committed consumer чекає поки транзакція не закрита.
Якщо payment-service тримає відкриту транзакцію — consumer LAG зростатиме.
read_uncommitted бачить все одразу — більший throughput але без EOS гарантій.
Для критичних consumers завжди read_committed.

**Слайд 10:**
Zombie fencing — захист від split-brain.
Якщо старий instance producer все ще активний після рестарту — це zombie.
Новий instance з тим самим transactional.id реєструється → Kafka fence zombie.
Старий отримує ProducerFencedException і зупиняється.
Гарантує що завжди тільки один активний producer per transactional.id.

**Слайд 11:**
Ключові висновки: EOS = idempotence + transactions + read_committed разом.
Без read_committed у consumer EOS не працює повністю.
transactional.id з prefix — production standard.
EOS має вищу latency і складність — використовувати тільки де дублікати неприйнятні.

**Слайд 12:**
Наступна гілка — Saga Pattern.
EOS гарантує атомарність у межах одного producer.
Saga Pattern вирішує розподілені транзакції між кількома сервісами.
Choreography через Kafka-події, compensation events при збоях.

## Тестові питання (до 10 питань)

**Питання 1:**
Producer з `enable.idempotence=true` надіслав повідомлення (seq=10).
Broker отримав але не встиг надіслати ACK → timeout.
Producer повторює з тим самим seq=10. Що відбудеться?

A) Broker запише дублікат
B) Broker відхилить дублікат → тільки одне повідомлення у топіку
C) Producer отримає DuplicateSequenceException
D) Kafka збільшить sequence до 11 автоматично

**Відповідь:** B —
З idempotence broker відстежує `(producer-id, partition, sequence-number)`.
Повторний seq=10 → duplicate → відхиляється → тільки одне запис.

---

**Питання 2:**
`kafkaTemplate.executeInTransaction { ... throw RuntimeException() }`.
Що відбудеться з повідомленнями відправленими всередині lambda?

A) Повідомлення committed — виключення не впливає після send()
B) Транзакція aborted — ABORT control batch у топіку
C) Повідомлення відкладаються до наступної успішної транзакції
D) RuntimeException ігнорується — транзакція committed

**Відповідь:** B —
RuntimeException всередині `executeInTransaction` → Spring Kafka викликає `abortTransaction()`.
ABORT control batch записується у топік.
`read_committed` consumers не побачать ці повідомлення.

---

**Питання 3:**
Consumer з `isolation-level: read-committed` читає `12.payments.processed`.
Payment-service почав транзакцію але ще не зробив commit (транзакція відкрита 10 секунд).
Що відбудеться з consumer?

A) Consumer прочитає uncommitted повідомлення
B) Consumer "призупиниться" перед першим uncommitted offset → LAG зростає
C) Consumer отримає помилку і зупиниться
D) Consumer пропустить uncommitted повідомлення і продовжить

**Відповідь:** B —
`read_committed`: consumer не читає повідомлення з відкритих транзакцій.
Consumer "зупиняється" (чекає) перед uncommitted offset.
LAG зростатиме поки транзакція не closed (committed або aborted).

---

**Питання 4:**
Яка мета `transaction-id-prefix` у Spring Kafka?

A) Шифрування транзакційних повідомлень
B) Унікальний ідентифікатор producer instance для відновлення і zombie fencing
C) Routing повідомлень до певних партицій
D) Збільшення throughput транзакційного producer

**Відповідь:** B —
`transaction-id-prefix` генерує унікальний `transactional.id` per partition.
Kafka використовує його при рестарті: abort незавершених транзакцій від попереднього instance.
Також: якщо новий producer реєструється → старий (zombie) fenced.

---

**Питання 5:**
Яка різниця між `read_uncommitted` і `read_committed` consumer?

A) read_uncommitted читає тільки committed; read_committed читає всі
B) read_uncommitted (default) читає всі повідомлення; read_committed тільки committed
C) Обидва читають однаково — різниця тільки в назві
D) read_committed читає швидше завдяки відсутності перевірок

**Відповідь:** B —
`read_uncommitted` (default): бачить всі записи включно з uncommitted і ABORTED.
`read_committed`: бачить тільки повідомлення з completed (committed) транзакцій.

---

**Питання 6:**
Що таке "Zombie Fencing" у Kafka транзакціях?

A) Блокування повідомлень від unauthorized producers
B) Механізм що запобігає zombie instance старого producer публікувати у активній транзакції
C) Видалення старих транзакцій після retention.ms
D) Перевірка schema compatibility для транзакційних топіків

**Відповідь:** B —
Zombie: старий producer instance після рестарту ще активний.
New instance реєструється з тим самим transactional.id → Kafka fence zombie.
Zombie отримує `ProducerFencedException`.
Гарантує один активний producer per transactional.id.

---

**Питання 7:**
Чому `acks=all` є обов'язковим для idempotent producer?

A) acks=all є обов'язковим для всіх типів producers
B) Idempotence без acks=all може давати втрату повідомлень навіть без дублікатів
C) Kafka автоматично встановлює acks=all для idempotent
D) Confluent вимагає acks=all для ліцензії

**Відповідь:** B —
Idempotence гарантує no duplicates.
Але без `acks=all` повідомлення може бути записано тільки на leader і загубитись при failover.
`acks=all` + idempotence = no duplicates + no data loss разом.

---

**Питання 8:**
Payment-service відправив `PaymentProcessedEvent` у транзакції але не зробив commit.
notification-service (read_committed) отримає подію?

A) Так, одразу після send()
B) Ні — тільки після commitTransaction()
C) Залежить від acks конфігурації
D) Тільки якщо notification-service використовує read_uncommitted

**Відповідь:** B —
`read_committed` consumer бачить повідомлення тільки після `commitTransaction()`.
До commit: повідомлення фізично у топіку але невидиме для `read_committed`.

---

**Питання 9:**
Яке з тверджень про Exactly-Once Semantics (EOS) в Kafka є НЕКОРЕКТНИМ?

A) EOS вимагає idempotent producer
B) EOS вимагає transactional producer для atomic publish
C) EOS гарантується тільки якщо consumer теж transactional
D) EOS вимагає read_committed consumer

**Відповідь:** C —
Consumer не потребує бути transactional для EOS.
Consumer потребує `read_committed` isolation level.
EOS = idempotent/transactional producer + read_committed consumer.

---

**Питання 10:**
Control batch типу ABORT в топіку `12.payments.processed`.
Яке повідомлення бачать різні consumers?

A) Обидва (read_committed і read_uncommitted) не бачать ABORTED повідомлення
B) read_uncommitted бачить binary ABORTED data; read_committed не бачить нічого
C) Обидва бачать однакове — control batch не впливає на читання
D) ABORT control batch видаляється через 1 хвилину

**Відповідь:** B —
`read_uncommitted`: бачить фізичні байти ABORTED повідомлення (binary, не parseble як business event).
`read_committed`: ігнорує ABORTED повідомлення і ABORT marker — нічого не бачить.
Фізично записи залишаються у log до retention.ms.