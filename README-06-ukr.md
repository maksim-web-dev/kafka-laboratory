# Branch 06 — Offsets & Commits

## `branch06_offsets_commits` — що вивчаємо

- **Manual commit** — явне підтвердження обробки через `ack.acknowledge()` замість автоматичного.
- **`enable.auto.commit: false`** — вимикає автоматичну фіксацію offset на рівні Kafka client.
- **`ack-mode: MANUAL_IMMEDIATE`** — Spring Kafka не накопичує підтверджень, фіксує одразу.
- **`Acknowledgment` параметр у `@KafkaListener`** — Spring передає об'єкт підтвердження в handler.
- **`ack.nack(Duration)`** — відхилення повідомлення з затримкою повторної доставки.
- **At-least-once delivery** — гарантія що повідомлення буде оброблено хоча б один раз.
- **At-most-once delivery** — ризик при auto-commit: offset фіксується до завершення обробки.
- **Симуляція збою** — ендпоінт `POST /api/notifications/simulate-failure/{count}` для демо.
- **`redelivered` лічильник** — поле в `/count` відповіді для відстеження повторних доставок.
- **Idempotency requirement** — при at-least-once обробник повинен бути ідемпотентним.

## Що змінилося порівняно з branch05

- Видалено `analytics-service` — фокус лише на commit-механізмі в `notification-service`.
- `enable.auto.commit` змінено на `false` — Kafka більше не фіксує offset автоматично.
- Доданий `ack-mode: MANUAL_IMMEDIATE` у `spring.kafka.listener`.
- `@KafkaListener` тепер приймає другий параметр `ack: Acknowledgment`.
- Додано `ack.acknowledge()` після успішної обробки.
- Додано `ack.nack(Duration.ofSeconds(2))` для симуляції збою з повторною доставкою.
- Новий ендпоінт `POST /api/notifications/simulate-failure/{count}` — запланувати N збоїв.
- `/count` відповідь тепер включає поле `redelivered` — кількість повторно доставлених.
- Назви топіків змінено на `06.orders.*`.

## Архітектура

```
┌──────────────────────────────────────────────────┐
│  order-service :8081                             │
│  POST /api/orders  →  kafkaTemplate.send()       │
└─────────────────┬────────────────────────────────┘
                  │
     ┌────────────┴───────────┐
     │                        │
 06.orders.created        06.orders.cancelled
   partitions: 3             partitions: 1
     │                        │
     └────────────┬───────────┘
                  │ notification-service-group
                  ▼
   notification-service-b06 :8082
   ┌─────────────────────────────────────────┐
   │ handleOrderCreated(record, ack):        │
   │   if failNextN > 0:                     │
   │     ack.nack(Duration.ofSeconds(2))     │
   │   else:                                 │
   │     process() → ack.acknowledge()       │
   └─────────────────────────────────────────┘
```

```
__consumer_offsets:
  notification-service-group | 06.orders.created | partition 0 → offset N
  (offset N фіксується тільки після ack.acknowledge(), не автоматично)
```

### Топіки та їх налаштування

- `06.orders.created` → партиції: 3, retention: 7 днів,
  читається `notification-service-group` з manual commit.
- `06.orders.cancelled` → партиції: 1, retention: 7 днів,
  читається `notification-service-group` з manual commit.

## Ключові концепції цієї гілки

### Auto-commit — ризик втрати повідомлення

З `enable.auto.commit=true` (default) Kafka автоматично фіксує offset кожні 5 секунд
(або при кожному `poll()`).
Якщо сервіс впаде **між отриманням** і **обробкою** — offset вже зафіксований:

```
Batch received:   [offset 10, offset 11, offset 12]
Auto-commit:      committed offset = 12  ← фіксується одразу
Processing:       offset 10 → OK
                  offset 11 → CRASH 💥
Restart:          committed offset = 12 → offset 11 вже ПРОПУЩЕНО (at-most-once)
```

### Manual commit — at-least-once

З `MANUAL_IMMEDIATE` offset фіксується лише після явного виклику `ack.acknowledge()`:

```
Batch received:   [offset 10, offset 11]
Processing:       offset 10 → OK → ack.acknowledge() → committed = 10
                  offset 11 → CRASH 💥 (ack не викликано)
Restart:          committed = 10 → offset 11 читається знову (at-least-once)
```

> Можливий дублікат: якщо `ack.acknowledge()` викликано, але сервіс впав **після** обробки —
> при рестарті повідомлення прийде знову. Тому обробник має бути **ідемпотентним**.

### ack.acknowledge() і ack.nack(Duration)

```kotlin
@KafkaListener(topics = ["06.orders.created"], groupId = "notification-service-group")
fun handleOrderCreated(
    record: ConsumerRecord<String, OrderCreatedEvent>,
    ack: Acknowledgment                                   // ← Spring передає Acknowledgment
) {
    if (failNextN.get() > 0) {
        failNextN.decrementAndGet()
        redeliveredCount.incrementAndGet()
        log.warn("[SIMULATED FAILURE] nacking partition={} offset={} — redelivery in 2s",
            record.partition(), record.offset())
        ack.nack(Duration.ofSeconds(2))                  // ← offset НЕ фіксується, retry через 2с
        return
    }
    // ... обробка ...
    ack.acknowledge()                                     // ← offset фіксується негайно
}
```

- `ack.nack(Duration)` — offset залишається незафіксованим.
  Kafka доставить це ж повідомлення знову через вказаний час.
  Наступні повідомлення в партиції **не читаються** поки поточне не підтверджено.
- `ack.acknowledge()` — offset фіксується. Consumer рухається далі.

### Конфігурація manual commit у application.yml

```yaml
# notification-service/application.yml
spring:
  kafka:
    consumer:
      enable-auto-commit: false       # ← Kafka client не фіксує автоматично
    listener:
      ack-mode: manual_immediate      # ← Spring Kafka очікує явного ack.acknowledge()
```

Без `ack-mode: manual_immediate` Spring Kafka самостійно викличе `acknowledge()`
після завершення listener-методу — навіть якщо ви не викликаєте вручну.

### Порівняння at-most-once vs at-least-once

```
Auto-commit (at-most-once):
  отримав → commit → обробляємо → crash → повідомлення ВТРАЧЕНО ❌

Manual commit (at-least-once):
  отримав → обробляємо → commit → crash? → читаємо знову ✅ (але можливий дублікат)

Exactly-once (branch12):
  транзакційний producer + read_committed consumer → ні втрат, ні дублікатів ✅
```

## Як запустити

```bash
docker compose -f docker-compose-06.yml up --build
```

Перевірити 4 контейнери:

```bash
docker compose -f docker-compose-06.yml ps
# kafka, kafka-ui, order-service-b06, notification-service-b06
```

## Як протестувати

### 1. Базова перевірка — manual commit працює

```bash
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-01","product":"Book","quantity":1,"totalAmount":25.00}'

curl http://localhost:8082/api/notifications/count
# {"instanceId":"notification-service-1","orders_created":1,"orders_cancelled":0,"redelivered":0,"total":1}
```

**Windows (PowerShell):**

```powershell
Invoke-RestMethod -Method POST -Uri http://localhost:8081/api/orders `
  -ContentType "application/json" `
  -Body '{"userId":"user-01","product":"Book","quantity":1,"totalAmount":25.00}'

Invoke-RestMethod http://localhost:8082/api/notifications/count
```

### 2. Головний демо-сценарій: симуляція збою → at-least-once

```bash
# Крок 1: Запланувати 2 збої для наступних повідомлень
curl -s -X POST http://localhost:8082/api/notifications/simulate-failure/2
# {"instanceId":"notification-service-1","scheduledNacks":2,"message":"Next 2 message(s) will be nacked and redelivered after 2s"}

# Крок 2: Надіслати замовлення
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-fail","product":"Doomed Order","quantity":1,"totalAmount":99.00}'

# Логи notification-service:
# [SIMULATED FAILURE] nacking partition=X offset=Y — redelivery in 2s
# (через 2с) [SIMULATED FAILURE] nacking ...
# (через 2с) ╔══ ORDER CREATED #1 ... ╚══
```

```bash
# Крок 3: Перевірити — orders_created=1, redelivered=2
curl http://localhost:8082/api/notifications/count
# {"orders_created":1,"orders_cancelled":0,"redelivered":2,"total":1}
```

### 3. Перевірити offset через CLI

```bash
# Під час збою (nack активний) — LAG > 0:
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group notification-service-group

# Після успішної обробки — LAG = 0
```

### 4. Kafka UI

Відкрити: `http://localhost:8080`

- `Consumer Groups` → `notification-service-group` → спостерігати зміну LAG під час nack.
- `Topics` → `06.orders.created` → бачимо offset після успішного `acknowledge()`.

## Як це працює всередині

### Producer (без змін порівняно з branch05)

Назви топіків змінились з `05.orders.*` на `06.orders.*` — лише рефакторинг нумерації.

### Consumer — notification-service (ключові зміни)

```kotlin
// OrderEventListener.kt — branch06
@KafkaListener(topics = ["06.orders.created"], groupId = "notification-service-group")
fun handleOrderCreated(
    record: ConsumerRecord<String, OrderCreatedEvent>,
    ack: Acknowledgment          // ← NEW: параметр Acknowledgment
) {
    if (failNextN.get() > 0) {
        failNextN.decrementAndGet()
        redeliveredCount.incrementAndGet()
        ack.nack(Duration.ofSeconds(2))   // ← NEW: відхиляємо, retry через 2с
        return
    }
    // ... логування ...
    ack.acknowledge()                      // ← NEW: явна фіксація offset
}
```

```yaml
# notification-service/application.yml — branch06
spring:
  kafka:
    consumer:
      enable-auto-commit: false       # ← NEW
    listener:
      ack-mode: manual_immediate      # ← NEW
```

## Структура проєкту (зміни відносно branch05)

```
kafka-laboratory/
├── docker-compose-06.yml                    ← без analytics-service, топіки 06.*
├── branch06_offsets_commits/
│   ├── notification-service/
│   │   └── src/main/resources/
│   │       └── application.yml              ← +ack-mode, +enable-auto-commit=false
│   │   └── src/main/kotlin/.../listener/
│   │       └── OrderEventListener.kt        ← +Acknowledgment param, +nack, +acknowledge
│   │   └── src/main/kotlin/.../controller/
│   │       └── NotificationController.kt    ← +simulate-failure endpoint, +redelivered у /count
│   └── order-service/                       ← топіки 06.orders.*
└── README06.md
```

## Що далі — branch07

- **acks=0, acks=1, acks=all** — три режими підтвердження producer і їх вплив на надійність.
- **Idempotent producer** — `enable.idempotence=true` та захист від дублікатів при retry.
- **linger.ms + batch.size** — батчинг повідомлень для збільшення throughput.
- **delivery.timeout.ms** — максимальний час спроб доставки одного повідомлення.
- **BenchmarkService** — порівняння продуктивності трьох конфігурацій.

---
---

## Слайди для презентації (11 слайдів)

**Слайд 1: Branch 06 — Offsets & Commits**
- Apache Kafka for Certification & Production
- Manual commit vs auto-commit

**Слайд 2: Agenda**
1. Проблема auto-commit — at-most-once ризик
2. Manual commit — at-least-once гарантія
3. `enable.auto.commit: false` + `ack-mode: MANUAL_IMMEDIATE`
4. `ack.acknowledge()` — явна фіксація offset
5. `ack.nack(Duration)` — відхилення з повторною доставкою
6. Idempotency requirement
7. Demo: simulate-failure → redelivery
8. At-most-once vs at-least-once vs exactly-once
9. CCDAK exam tips

**Слайд 3: Проблема Auto-Commit**
- `enable.auto.commit=true` (default) — Kafka фіксує offset кожні 5 секунд
- Offset фіксується **до** завершення обробки
- Якщо сервіс впаде під час обробки — повідомлення втрачено
- Це at-most-once: кожне повідомлення обробляється **не більше** одного разу

**Слайд 4: Manual Commit — At-Least-Once**
- `enable.auto.commit: false` + `ack-mode: MANUAL_IMMEDIATE`
- Offset фіксується лише після `ack.acknowledge()`
- Якщо сервіс впаде до `acknowledge()` — повідомлення прийде знову
- Це at-least-once: кожне повідомлення обробляється **хоча б** один раз (можливий дублікат)

**Слайд 5: Acknowledgment API**
- `ack.acknowledge()` — фіксує offset негайно (MANUAL_IMMEDIATE)
- `ack.nack(Duration)` — відхиляє повідомлення, повторна доставка через Duration
- Під час nack-стану: наступні повідомлення в партиції не читаються
- Spring передає `Acknowledgment` як параметр у `@KafkaListener`

**Слайд 6: Демо — Simulate Failure**
- `POST /api/notifications/simulate-failure/2` — запланувати 2 збої
- Перше повідомлення → nack → через 2с → nack → через 2с → acknowledge
- `/count`: orders_created=1, redelivered=2
- LAG > 0 під час nack, LAG = 0 після acknowledge

**Слайд 7: Idempotency Requirement**
- При at-least-once дублікати — не виняток, а **норма**
- Обробник повинен бути ідемпотентним: повторне виконання не змінює стан
- Приклади ідемпотентності: INSERT OR IGNORE, UPDATE WHERE state=X, check orderId
- Якщо бізнес-логіка не ідемпотентна → потрібен exactly-once (branch12)

**Слайд 8: MANUAL_IMMEDIATE vs MANUAL**
- `MANUAL_IMMEDIATE` — commit відбувається одразу при виклику ack.acknowledge()
- `MANUAL` — commit відбувається при наступному poll() (batch-режим)
- Для більшості use-cases — `MANUAL_IMMEDIATE` простіше і надійніше
- `BATCH` — один ack для всього batch одразу

**Слайд 9: Стратегії Delivery**
- **at-most-once**: auto-commit → проста реалізація, можлива втрата
- **at-least-once**: manual commit → гарантія доставки, можливий дублікат
- **exactly-once**: транзакції (branch12) → найскладніше, максимальна надійність
- Вибір залежить від бізнес-вимог (фінанси vs аналітика)

**Слайд 10: Key Takeaways**
- `enable.auto.commit=false` + `ack-mode=MANUAL_IMMEDIATE` — production-стандарт
- `ack.acknowledge()` після успішної обробки — гарантія at-least-once
- `ack.nack(Duration)` — controlled retry без втрати повідомлення
- Idempotency — обов'язкова вимога при at-least-once
- Дублікати — норма в at-least-once системах

**Слайд 11: What's Next — Branch 07: Producer Configuration**
- acks=0 vs acks=1 vs acks=all — три режими підтвердження
- Idempotent producer — захист від дублікатів при retry
- linger.ms + batch.size — батчинг для throughput
- BenchmarkService — вимірювання продуктивності

## Текст для презентації (скрипт)

**Слайд 1:**
Вітаємо у шостій гілці.
До цього ми вивчали як повідомлення читаються і розподіляються між consumer groups.
Сьогодні розберемо одне з найважливіших питань для production: що відбувається з offset-ом
якщо сервіс падає під час обробки?

**Слайд 2:**
Пройдемо дев'ять тем: від проблеми auto-commit до стратегій доставки.
Головна ідея: offset потрібно фіксувати тільки після підтвердженої обробки.

**Слайд 3:**
Auto-commit — зручна за замовчуванням поведінка.
Kafka фіксує offset кожні 5 секунд незалежно від того обробили ми повідомлення чи ні.
Якщо сервіс впаде між авто-commit і завершенням обробки — повідомлення буде пропущено навіки.
Це називається at-most-once: краще не отримати, ніж отримати двічі.

**Слайд 4:**
Manual commit перевертає логіку: спочатку обробляємо, потім фіксуємо.
Якщо сервіс впаде до ack.acknowledge() — offset не зафіксований.
При рестарті Kafka доставить повідомлення знову.
Так забезпечується at-least-once: повідомлення точно буде оброблено, але можливо і двічі.

**Слайд 5:**
Spring Kafka надає Acknowledgment через параметр listener-методу.
ack.acknowledge() фіксує offset негайно — це MANUAL_IMMEDIATE режим.
ack.nack(Duration) відхиляє повідомлення і повторна доставка відбудеться через вказаний час.
Поки повідомлення очікує повторної доставки — наступні в партиції не читаються.

**Слайд 6:**
У демо ми плануємо два збої через POST /simulate-failure/2.
Перше замовлення отримується — nack — через 2 секунди знову nack — через 2 секунди успіх.
У лічильнику: orders_created=1 але redelivered=2 — ми бачимо скільки разів перепробували.
Це точно at-least-once в дії.

**Слайд 7:**
Головний наслідок at-least-once: обробник повинен бути ідемпотентним.
Якщо те саме замовлення прийде двічі — повторна обробка не повинна ламати систему.
Наприклад: перевіряти чи orderId вже існує в базі перед вставкою.
Якщо це неможливо технічно — потрібен exactly-once (branch12 з транзакціями).

**Слайд 8:**
MANUAL_IMMEDIATE — найпоширеніший режим для production.
Commit відбувається одразу при виклику acknowledge — без затримки.
MANUAL режим накопичує і фіксує при наступному poll — для складних batch сценаріїв.
Для більшості мікросервісів MANUAL_IMMEDIATE простіший і безпечніший.

**Слайд 9:**
Три стратегії доставки: at-most-once для некритичних логів і метрик.
At-least-once — стандарт для більшості бізнес-сервісів з ідемпотентними обробниками.
Exactly-once — для фінансових транзакцій де дублікати неприйнятні взагалі.
Вибирайте стратегію виходячи з бізнес-вимог а не технічних уподобань.

**Слайд 10:**
Запам'ятайте: enable.auto.commit=false і ack-mode=MANUAL_IMMEDIATE — це production-конфігурація.
Ніколи не покладайтесь на auto-commit для критичних бізнес-подій.
Завжди перевіряйте що ваш обробник ідемпотентний при manual commit.

**Слайд 11:**
Наступна гілка — Producer Configuration.
Ми зосередимось на стороні виробника: acks, idempotence, linger.ms, batch.size.
Побачимо як BenchmarkService вимірює продуктивність трьох конфігурацій.

## Тестові питання (до 10 питань)

**Питання 1:**
Consumer читає повідомлення і сервіс падає **після** автоматичного commit але **до** завершення обробки.
Яку delivery гарантію демонструє цей сценарій?

A) At-least-once
B) At-most-once
C) Exactly-once
D) No guarantee

**Відповідь:** B —
Auto-commit фіксує offset незалежно від обробки.
Якщо сервіс впаде після commit але до обробки — повідомлення втрачено назавжди (at-most-once).

---

**Питання 2:**
Яка комбінація налаштувань активує manual commit у Spring Kafka?

A) `ack-mode: auto` + `enable.auto.commit: true`
B) `ack-mode: manual_immediate` + `enable.auto.commit: false`
C) `ack-mode: batch` + `enable.auto.commit: false`
D) Тільки `enable.auto.commit: false`

**Відповідь:** B —
`enable.auto.commit: false` вимикає авто-commit на рівні Kafka client.
`ack-mode: manual_immediate` вказує Spring Kafka фіксувати тільки після явного `ack.acknowledge()`.

---

**Питання 3:**
`ack.nack(Duration.ofSeconds(2))` викликано для повідомлення на offset=10, partition=1.
Що відбудеться з повідомленнями на offset=11, 12, 13 (та сама партиція)?

A) Вони будуть прочитані одразу
B) Вони очікуватимуть поки offset=10 не буде підтверджений
C) Вони будуть прочитані але без commit
D) Kafka видалить їх

**Відповідь:** B —
`nack` не фіксує offset.
Consumer не перейде до наступних повідомлень в партиції поки поточне не підтверджено або перенаправлено.

---

**Питання 4:**
Consumer отримав повідомлення, успішно обробив, викликав `ack.acknowledge()`, потім впав.
Що відбудеться при рестарті?

A) Повідомлення буде доставлено знову (at-least-once)
B) Повідомлення не буде доставлено (offset вже зафіксований)
C) Kafka визначить чи потрібна повторна доставка автоматично
D) Consumer почне читати з початку топіку

**Відповідь:** B —
`ack.acknowledge()` вже зафіксував offset.
При рестарті consumer починає з наступного offset — повідомлення не повторюється.

---

**Питання 5:**
Notification-service отримав замовлення двічі через at-least-once retry.
Яка техніка дозволяє уникнути відправки email двічі одному користувачу?

A) Збільшити `max.poll.records`
B) Зберігати orderId у базі і перевіряти перед відправкою (idempotency check)
C) Використати `acks=all`
D) Зменшити `linger.ms`

**Відповідь:** B —
Idempotency check: перед обробкою перевіряємо чи вже оброблено цей orderId.
Якщо так — пропускаємо без помилки. Це стандартна техніка для at-least-once систем.

---

**Питання 6:**
Яка різниця між `MANUAL_IMMEDIATE` і `MANUAL` ack-mode у Spring Kafka?

A) `MANUAL_IMMEDIATE` — commit після кожного повідомлення; `MANUAL` — після batch
B) `MANUAL_IMMEDIATE` — commit при наступному poll; `MANUAL` — відразу
C) Обидва однакові, відрізняються лише назвою
D) `MANUAL` використовує async commit; `MANUAL_IMMEDIATE` — sync

**Відповідь:** A —
`MANUAL_IMMEDIATE` фіксує offset одразу при виклику `acknowledge()`.
`MANUAL` накопичує підтвердження і фіксує при наступному `poll()`.

---

**Питання 7:**
Яка з delivery гарантій є стандартом для більшості мікросервісів у production?

A) At-most-once — найпростіша реалізація
B) At-least-once з ідемпотентними обробниками
C) Exactly-once завжди
D) No guarantee для максимальної швидкості

**Відповідь:** B —
At-least-once з ідемпотентними обробниками — золотий стандарт.
Exactly-once складніше і повільніше; at-most-once ризикований для бізнес-даних.

---

**Питання 8:**
Яке значення `redelivered` у `/count` відповіді якщо ми запланували 2 збої і надіслали 1 замовлення?

A) 0
B) 1
C) 2
D) 3

**Відповідь:** C —
Кожен виклик `ack.nack()` інкрементує `redeliveredCount`.
2 заплановані збої → 2 виклики nack → `redelivered=2`, але `orders_created=1`.

---

**Питання 9:**
Чому auto-commit небезпечний для фінансових транзакцій?

A) Auto-commit занадто повільний для фінансів
B) Offset фіксується до завершення обробки → при збої транзакція втрачається
C) Auto-commit не підтримує партиції
D) Kafka не дозволяє auto-commit для топіків з `retention.ms > 1 день`

**Відповідь:** B —
У фінансових транзакціях втрата повідомлення означає незарахований платіж.
Auto-commit може зафіксувати offset до завершення обробки — і при збої оплата губиться.

---

**Питання 10:**
Як налаштувати consumer щоб він НІКОЛИ не фіксував offset автоматично у Spring Kafka?

A) `spring.kafka.consumer.enable-auto-commit: false`
B) `spring.kafka.listener.ack-mode: manual_immediate`
C) Обидва разом: A + B
D) `spring.kafka.consumer.auto-offset-reset: none`

**Відповідь:** C —
Одного `enable-auto-commit: false` недостатньо: Spring Kafka може все одно автоматично викликати
`acknowledge()` якщо `ack-mode` не встановлено явно.
Потрібні обидва налаштування разом.