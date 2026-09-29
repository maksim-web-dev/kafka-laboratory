# Branch 07 — Producer Configuration

## `branch07_producer_configuration` — що вивчаємо

- **acks=0, acks=1, acks=all** — три рівні підтвердження запису та їх вплив на надійність і latency.
- **ISR (In-Sync Replicas)** — множина брокерів що синхронізовані з лідером партиції.
- **Idempotent producer** — `enable.idempotence=true` з `producer-id` і `sequence number`.
- **Захист від дублікатів при retry** — broker відхиляє повторні send з однаковим sequence.
- **`linger.ms`** — час очікування для накопичення більшого batch перед відправкою.
- **`batch.size`** — максимальний розмір batch у байтах (16 KB / 32 KB / 64 KB).
- **`delivery.timeout.ms`** — загальний ліміт часу для однієї спроби відправки з retry.
- **`retries`** — кількість повторних спроб при тимчасових помилках мережі.
- **BenchmarkService** — порівняння throughput і latency трьох конфігурацій.
- **`max.in.flight.requests.per.connection`** — кількість паралельних незаверше­них запитів.

## Що змінилося порівняно з branch06

- `notification-service` залишається — повертаємось до auto-commit (фокус на producer-конфіг).
- `order-service` отримує production-safe default producer: `acks=all`, `enable.idempotence=true`.
- Доданий `BenchmarkService` та `BenchmarkController` — нові ендпоінти для вимірювання.
- Новий топік `07.benchmark` — для benchmark-повідомлень (не бізнес-дані).
- Три конфігурації producer: `acks0`, `acks1`, `acks-all` для порівняння.
- `linger.ms=20` і `batch.size=65536` — батчинг для `acks-all` конфігурації.
- Назви топіків змінено на `07.orders.*`.

## Архітектура

```
┌─────────────────────────────────────────────────────────────┐
│  order-service :8081                                        │
│                                                             │
│  Default producer (production-safe):                        │
│    acks=all, enable.idempotence=true, linger.ms=20          │
│                                                             │
│  BenchmarkService:                                          │
│    acks0Producer  → acks=0, linger=0, batch=16KB            │
│    acks1Producer  → acks=1, linger=5ms, batch=32KB          │
│    acksAllProducer → acks=all, idempotent, linger=20ms      │
└───────┬────────────────┬────────────────────────────────────┘
        │                │
  07.orders.created   07.benchmark
   (3 partitions)     (1 partition)
        │
        ▼
  notification-service-b07 :8082
  (auto-commit, тільки для перевірки що повідомлення надходять)
```

### Топіки та їх налаштування

- `07.orders.created` → партиції: 3, retention: 7 днів, producer: default (acks=all, idempotent).
- `07.orders.cancelled` → партиції: 1, retention: 7 днів.
- `07.benchmark` → партиції: 1, retention: 1 година, використовується лише для benchmark.

## Ключові концепції цієї гілки

### acks — три режими підтвердження

```
Producer → Broker Leader (partition N)
              │
  acks=0:     │  Нічого не чекаємо — "fire and forget"
  acks=1:     │  Чекаємо "OK" від leader
  acks=all:   │  Чекаємо "OK" від leader + усіх ISR

ISR = In-Sync Replicas: брокери що підтримують реплікацію в актуальному стані
```

- `acks=0` — Producer не чекає жодного підтвердження.
  Мінімальна latency, максимальний throughput.
  Повідомлення може бути втрачено якщо broker недоступний.
  Підходить для: метрики, логи, де окрема втрата прийнятна.
- `acks=1` — Producer чекає підтвердження від leader-брокера.
  Баланс між надійністю і швидкістю.
  Повідомлення може бути втрачено якщо leader впаде **до реплікації** на follower.
  Підходить для: внутрішні події, де дублікати допустимі.
- `acks=all` — Producer чекає підтвердження від leader **і всіх ISR**.
  Максимальна надійність, вища latency.
  При `min.insync.replicas=2` і 2+ ISR: втрата неможлива без втрати даних брокера.
  Підходить для: критичні бізнес-події (замовлення, платежі).

### Idempotent Producer — захист від дублікатів

Без idempotence: якщо producer надіслав повідомлення, але timeout до отримання ACK —
producer повторює відправлення → broker записує **дублікат**.

```
Без idempotence (acks=1, retries=3):
  Producer: send(orderId=ABC) → seq не відстежується
  Timeout (повідомлення дійшло, але ACK загублено)
  Retry: send(orderId=ABC) → broker записує ВДРУГЕ ← дублікат ❌

З enable.idempotence=true:
  Producer отримує unique producer-id від broker
  Кожне повідомлення: producer-id + sequence-number (per partition)
  Retry: send(orderId=ABC, seq=5) → broker: "вже маємо seq=5 від цього producer" → skip ✓
```

**Вимоги для idempotence:**

```yaml
producer:
  acks: all                                   # обов'язково acks=all
  retries: 2147483647                         # > 0
  properties:
    enable.idempotence: true
    max.in.flight.requests.per.connection: 5  # ≤ 5
```

### linger.ms + batch.size — батчинг

```
linger.ms=0 (default):
  msg-1 надходить → одразу відправляємо batch[msg-1]  ← 1 мережевий запит на повідомлення

linger.ms=20:
  t=0ms:  msg-1 → чекаємо 20ms
  t=8ms:  msg-2 → додаємо до batch
  t=15ms: msg-3 → додаємо до batch
  t=20ms: → відправляємо batch[msg-1, msg-2, msg-3] ← 1 запит замість 3, вища компресія
```

- `batch.size` — максимальний розмір batch у байтах.
  Batch відправляється або коли заповнений, або коли `linger.ms` минув.
  Збільшення batch.size підвищує throughput але збільшує latency першого повідомлення.

### Три конфігурації для benchmark

```yaml
# acks=0: fire-and-forget — максимальна швидкість
acks: "0"
linger.ms: 0
batch.size: 16384   # 16 KB
retries: 0

# acks=1: баланс
acks: "1"
linger.ms: 5
batch.size: 32768   # 32 KB
retries: 3

# acks=all + idempotent: максимальна надійність
acks: "all"
linger.ms: 20
batch.size: 65536   # 64 KB
enable.idempotence: true
max.in.flight.requests.per.connection: 5
delivery.timeout.ms: 30000
```

### delivery.timeout.ms

```
delivery.timeout.ms=30000 (30с):
  Загальний час від першого send() до останнього retry або помилки.
  Якщо всі retry не вклались у 30с → DeliveryTimeoutException

retry.backoff.ms=100:
  Пауза між retry-спробами

Зв'язок:
  delivery.timeout.ms ≥ linger.ms + request.timeout.ms
```

## Як запустити

```bash
docker compose -f docker-compose-07.yml up --build
```

Перевірити 4 контейнери:

```bash
docker compose -f docker-compose-07.yml ps
# kafka, kafka-ui, order-service-b07, notification-service-b07
```

## Як протестувати

### 1. Benchmark одного режиму

```bash
# acks=0: fire-and-forget, 200 повідомлень
curl -s -X POST "http://localhost:8081/api/benchmark/run?mode=acks0&count=200" | python3 -m json.tool

# acks=1
curl -s -X POST "http://localhost:8081/api/benchmark/run?mode=acks1&count=200" | python3 -m json.tool

# acks=all + idempotent
curl -s -X POST "http://localhost:8081/api/benchmark/run?mode=acks-all&count=200" | python3 -m json.tool
```

Очікувана відповідь:

```json
{
  "mode": "acks0",
  "acksConfig": "0",
  "idempotent": false,
  "lingerMs": 0,
  "batchSizeBytes": 16384,
  "messageCount": 200,
  "totalDurationMs": 45,
  "throughputMsgPerSec": 4444.4,
  "avgLatencyMs": 0.22
}
```

### 2. Порівняти всі три режими (Linux)

```bash
curl -s -X POST "http://localhost:8081/api/benchmark/compare?count=200"
```

**Windows (PowerShell):**

```powershell
Invoke-RestMethod -Method POST -Uri "http://localhost:8081/api/benchmark/compare?count=200"
```

Очікуваний результат (з одним broker):

```
mode      | throughput (msg/s) | avgLatency (ms)
acks0     | ~5000              | ~0.2
acks1     | ~1000              | ~1.0
acks-all  | ~800               | ~1.2
```

> Примітка: з одним broker різниця між acks=1 і acks=all мінімальна —
> лише один ISR, обидва чекають одного підтвердження.
> У production з 3 broker і min.insync.replicas=2 різниця суттєвіша.

### 3. Звичайне замовлення через production-safe producer

```bash
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-01","product":"Book","quantity":1,"totalAmount":25.00}'
```

```bash
# Логи order-service покажуть:
# OrderCreated published → topic=07.orders.created, partition=X, offset=Y, key=user-01
```

### 4. Kafka UI — producer метрики

Відкрити: `http://localhost:8080`

- `Topics` → `07.benchmark` → бачимо повідомлення від benchmark з різними конфігураціями.
- `Brokers` → `Metrics` → шукати `record-send-rate`, `record-error-rate`, `batch-size-avg`.

## Як це працює всередині

### Producer — order-service (ключові зміни)

Production-safe конфігурація за замовчуванням:

```yaml
# order-service/application.yml — branch07
spring:
  kafka:
    producer:
      acks: all                              # ← NEW: максимальна надійність
      properties:
        enable.idempotence: true             # ← NEW: захист від дублікатів
        max.in.flight.requests.per.connection: 5
        delivery.timeout.ms: 30000
        linger.ms: 20                        # ← NEW: батчинг
        batch.size: 65536                    # ← NEW: 64 KB batch
        retries: 2147483647
```

```kotlin
// BenchmarkService.kt (новий)
fun runBenchmark(mode: String, count: Int): BenchmarkResult {
    val producer = when(mode) {
        "acks0"    -> acks0Producer
        "acks1"    -> acks1Producer
        "acks-all" -> acksAllProducer
        else -> throw IllegalArgumentException("Unknown mode: $mode")
    }
    val start = System.currentTimeMillis()
    repeat(count) { i ->
        producer.send("07.benchmark", "bench-key-$i", "msg-$i").get()
    }
    val duration = System.currentTimeMillis() - start
    return BenchmarkResult(mode, count, duration, ...)
}
```

### Consumer — notification-service

Повернуто auto-commit (фокус на producer-стороні):
слухає `07.orders.created`, логує отримані повідомлення — без змін логіки.

## Структура проєкту (зміни відносно branch06)

```
kafka-laboratory/
├── docker-compose-07.yml                              ← без analytics, топіки 07.*
├── branch07_producer_configuration/
│   ├── order-service/
│   │   └── src/main/resources/
│   │       └── application.yml                        ← acks=all, idempotence, linger, batch
│   │   └── src/main/kotlin/.../service/
│   │       ├── OrderService.kt                        ← стандартна публікація (без змін)
│   │       └── BenchmarkService.kt                    ← NEW: runBenchmark(), compareAll()
│   │   └── src/main/kotlin/.../config/
│   │       └── KafkaTopicConfig.kt                    ← +07.benchmark topic
│   │   └── src/main/kotlin/.../model/
│   │       └── BenchmarkResult.kt                     ← NEW: data class
│   │   └── src/main/kotlin/.../controller/
│   │       └── BenchmarkController.kt                 ← NEW: /api/benchmark/run, /compare
│   └── notification-service/                          ← auto-commit (незмінний)
└── README07.md
```

## Що далі — branch08

- **CooperativeStickyAssignor** — incremental rebalance замість stop-the-world EAGER.
- **Static Group Membership** — `group.instance.id` щоб уникнути зайвих rebalance при рестарті.
- **`max.poll.records`** та **`max.poll.interval.ms`** — тюнінг batch-розміру.
- **`session.timeout.ms`** і **`heartbeat.interval.ms`** — налаштування виявлення збоїв.
- **Graceful shutdown** — `server.shutdown=graceful` + `@PreDestroy`.

---
---

## Слайди для презентації (11 слайдів)

**Слайд 1: Branch 07 — Producer Configuration**
- Apache Kafka for Certification & Production
- acks, idempotence, batching

**Слайд 2: Agenda**
1. acks=0, acks=1, acks=all — три рівні надійності
2. ISR — In-Sync Replicas
3. min.insync.replicas та NotEnoughReplicasException
4. Idempotent Producer — sequence numbers
5. linger.ms + batch.size — батчинг
6. delivery.timeout.ms + retries
7. Три конфігурації для різних use-cases
8. BenchmarkService — вимірювання
9. Key takeaways & CCDAK

**Слайд 3: acks — Три Рівні Підтвердження**
- `acks=0`: fire-and-forget, можлива втрата, максимальна швидкість
- `acks=1`: leader підтверджує, можлива втрата при failover, середня швидкість
- `acks=all`: всі ISR підтверджують, мінімальна втрата, менша швидкість

**Слайд 4: ISR та min.insync.replicas**
- ISR: брокери що синхронізовані з leader-ом протягом replica.lag.time.max.ms
- `min.insync.replicas=2`: producer отримає помилку якщо < 2 ISR доступні
- Типова production конфігурація: 3 broker, RF=3, min.insync.replicas=2
- "Quorum write" — стійкість до відмови одного broker при збереженні даних

**Слайд 5: Idempotent Producer**
- Без idempotence: timeout + retry → дублікат в Kafka
- `enable.idempotence=true`: producer-id + sequence number per partition
- Broker відхиляє дублікати автоматично (same producer-id + sequence)
- Вимоги: acks=all, retries>0, max.in.flight≤5

**Слайд 6: linger.ms + batch.size**
- `linger.ms=0`: відправляємо одразу — низька latency, малий batch
- `linger.ms=20`: чекаємо 20мс — вищий throughput, більший batch
- `batch.size=65536`: максимум 64KB в одному batch
- Batch відправляється: або заповнений, або `linger.ms` минув

**Слайд 7: delivery.timeout.ms**
- Загальний час від send() до успіху або помилки
- Включає всі retry і backoff паузи
- Якщо вичерпано → DeliveryTimeoutException
- Правило: delivery.timeout ≥ linger.ms + request.timeout.ms

**Слайд 8: Три Конфігурації**
- `acks=0, linger=0, batch=16KB` — метрики, логи (втрата прийнятна)
- `acks=1, linger=5ms, batch=32KB` — внутрішні події (дублікати допустимі)
- `acks=all, linger=20ms, batch=64KB, idempotent` — бізнес-події (фінанси, замовлення)

**Слайд 9: BenchmarkService Results**
- acks0: ~5000 msg/s, latency ~0.2ms (з одним broker)
- acks1: ~1000 msg/s, latency ~1.0ms
- acks-all: ~800 msg/s, latency ~1.2ms (але з батчингом linger=20ms)
- З 3 брокерами і min.insync.replicas=2 різниця acks1 vs acks-all суттєвіша

**Слайд 10: Key Takeaways**
- `acks=all` + `enable.idempotence=true` — production-стандарт для бізнес-подій
- `linger.ms + batch.size` — trade-off між latency і throughput
- Idempotence захищає від дублікатів при retry на рівні broker
- `delivery.timeout.ms` — загальний ліміт часу для однієї спроби доставки
- З одним broker acks=1 vs acks=all — мінімальна різниця

**Слайд 11: What's Next — Branch 08: Consumer Configuration**
- CooperativeStickyAssignor — incremental rebalance
- Static Group Membership — менше rebalance при рестарті
- max.poll.records, session.timeout.ms — тюнінг
- Graceful shutdown

## Текст для презентації (скрипт)

**Слайд 1:**
Сьома гілка — Producer Configuration.
До цього ми розбирали consumer-сторону: commit, lag, групи.
Тепер фокус на producer: як Kafka гарантує що повідомлення дійшло до broker.

**Слайд 2:**
Вивчаємо три рівні підтвердження, ідемпотентність, батчинг і вимірювання.
Головна ідея: конфігурація producer визначає баланс між швидкістю і надійністю.

**Слайд 3:**
acks=0: producer відправляє і не чекає жодної відповіді.
Максимальна швидкість але якщо broker недоступний — повідомлення губиться назавжди.
acks=1: чекаємо підтвердження від leader-брокера.
Повідомлення може загубитись якщо leader впав до реплікації на follower.
acks=all: чекаємо підтвердження від усіх ISR — найнадійніший режим.

**Слайд 4:**
ISR — In-Sync Replicas: брокери що тримаються синхронізованими з лідером.
min.insync.replicas визначає скільки ISR мінімум мають підтвердити.
З трьома брокерами і min.insync.replicas=2: ми можемо дозволити собі один broker failure.
Якщо залишилось менше мінімуму ISR — producer отримає NotEnoughReplicasException.

**Слайд 5:**
Idempotent producer вирішує проблему дублікатів при retry.
Broker видає producer-id при реєстрації.
Кожне повідомлення має sequence number унікальний для producer+partition.
При retry: якщо broker вже має цей sequence — він мовчки ігнорує дублікат.

**Слайд 6:**
linger.ms і batch.size — механізм батчинга.
З linger=0 кожне повідомлення йде окремим мережевим запитом.
З linger=20ms producer чекає і накопичує кілька повідомлень в один batch.
Менше мережевих запитів, краще стиснення, вищий throughput — за ціну трохи більшої latency.

**Слайд 7:**
delivery.timeout.ms — загальний бюджет часу на доставку одного повідомлення.
Якщо за цей час не вдалось відправити з усіма retry — producer кидає виключення.
Зазвичай 30-120 секунд для production.
Важливо щоб delivery.timeout був більше ніж linger.ms плюс request.timeout.ms.

**Слайд 8:**
Три конфігурації для трьох типів use-case.
acks=0 для метрик і логів де окрема втрата не критична.
acks=1 для внутрішніх подій де швидкість важливіша за ідеальну надійність.
acks=all з ідемпотентністю — для будь-яких бізнес-подій де гроші або дані.

**Слайд 9:**
BenchmarkService дозволяє виміряти різницю.
З одним broker різниця між acks=1 і acks=all невелика — лише один ISR.
У production з трьома брокерами acks=all помітно повільніше.
Але для критичних подій це прийнятна ціна за надійність.

**Слайд 10:**
Основні висновки: acks=all і idempotence — це production-стандарт для бізнес-даних.
linger.ms і batch.size — інструменти оптимізації throughput.
delivery.timeout.ms обмежує загальний час спроб доставки.
Вибирайте конфігурацію виходячи з бізнес-вимог до надійності.

**Слайд 11:**
Наступна гілка — Consumer Configuration.
Вивчимо CooperativeStickyAssignor для м'якого rebalance.
Static Group Membership щоб зменшити кількість rebalance при рестарті сервісів.
І graceful shutdown — як правильно завершувати роботу consumer.

## Тестові питання (до 10 питань)

**Питання 1:**
Producer надіслав повідомлення з `acks=1`. Leader broker підтвердив але впав
**до** реплікації на follower. Яка втрата відбудеться?

A) Повідомлення не буде втрачено — acks=1 гарантує надійність
B) Повідомлення буде втрачено — follower не має копії
C) Повідомлення буде відновлено з producer buffer
D) Kafka автоматично повторить відправку

**Відповідь:** B —
`acks=1` гарантує лише що leader отримав повідомлення.
Якщо leader впаде до реплікації — follower стає новим leader **без цього повідомлення**.

---

**Питання 2:**
Яке мінімальне значення `min.insync.replicas` для топіку з 3 репліками
що забезпечить стійкість до відмови одного broker?

A) 1
B) 2
C) 3
D) Залежить від `acks`

**Відповідь:** B —
З `min.insync.replicas=2` і 3 репліками: якщо 1 broker відпадає — залишається 2 ISR ≥ min.
Якщо 2 broker відпадають — залишається 1 ISR < min → `NotEnoughReplicasException`.

---

**Питання 3:**
Producer відправив повідомлення (seq=42), отримав timeout (broker записав, але ACK загублено),
повторив відправку (seq=42). Broker отримав дублікат. Що відбудеться з `enable.idempotence=true`?

A) Broker запише обидва — дублікат з'явиться в топіку
B) Broker відхилить другий запис — seq=42 вже існує для цього producer
C) Producer кине DuplicateKeyException
D) Consumer отримає обидва, але з однаковим offset

**Відповідь:** B —
З idempotence broker відстежує `(producer-id, partition, sequence-number)`.
Дублікат з тим самим sequence number мовчки ігнорується.

---

**Питання 4:**
`linger.ms=20` і `batch.size=65536`. Producer отримав 3 повідомлення загальним розміром 70KB.
Скільки мережевих запитів буде відправлено?

A) 1 — batch об'єднає всі три
B) 2 — перший batch заповниться до 64KB, другий — залишок
C) 3 — кожне повідомлення окремо (batch.size перевищено)
D) Залежить від linger.ms

**Відповідь:** B —
Batch відправляється коли досягає `batch.size` (64KB) або минає `linger.ms`.
70KB не вміщається в один batch → перший batch ~64KB, другий ~6KB.

---

**Питання 5:**
Яке налаштування є обов'язковим для `enable.idempotence=true`?

A) `acks=1`
B) `acks=all`
C) `linger.ms > 0`
D) `batch.size > 16384`

**Відповідь:** B —
Idempotent producer вимагає `acks=all`, `retries > 0`,
і `max.in.flight.requests.per.connection ≤ 5`.
Без `acks=all` Kafka кине `ConfigException`.

---

**Питання 6:**
Producer виставив `delivery.timeout.ms=30000`. Спроба відправки зайняла 35 секунд через retry.
Що повернеться в `whenComplete`?

A) Успішний результат — timeout відраховується від кожної retry окремо
B) `TimeoutException` — загальний час перевищено
C) `DeliveryTimeoutException` — Kafka-специфічний тип
D) Повідомлення буде відправлено але без підтвердження

**Відповідь:** C —
`delivery.timeout.ms` — загальний час від першого `send()` до успіху або відмови.
Якщо всі retry вклались у 30с → OK. Якщо ні → `DeliveryTimeoutException`.

---

**Питання 7:**
Яка конфігурація підходить для відправки метрик моніторингу (втрата 1-2% прийнятна)?

A) `acks=all, enable.idempotence=true`
B) `acks=0, linger.ms=0, retries=0`
C) `acks=1, linger.ms=5`
D) `acks=all, linger.ms=0`

**Відповідь:** B —
Для некритичних метрик: `acks=0` — найвища швидкість.
`retries=0` — немає сенсу повторювати fire-and-forget.
`linger.ms=0` — мінімальна затримка.

---

**Питання 8:**
Чому `max.in.flight.requests.per.connection ≤ 5` є вимогою для idempotent producer?

A) Більше 5 паралельних запитів перевантажує broker
B) При > 5 паралельних запитів порядок sequence numbers може порушитись → помилки дедублікації
C) Kafka не підтримує > 5 паралельних з'єднань
D) Це обмеження TCP стека

**Відповідь:** B —
Ідемпотентність гарантується тільки якщо sequence numbers приходять в правильному порядку.
З > 5 in-flight requests можливий out-of-order delivery → broker не може правильно дедублювати.

---

**Питання 9:**
В production кластері (3 broker, RF=3, min.insync.replicas=2) використовується `acks=all`.
Один broker недоступний. Що відбудеться з producer?

A) Producer отримає помилку — ISR < 3
B) Producer продовжить працювати — ISR = 2 ≥ min.insync.replicas
C) Kafka автоматично знизить до acks=1
D) Producer почне буферизувати повідомлення до відновлення broker

**Відповідь:** B —
min.insync.replicas=2, ISR=2 (два broker ще живі) ≥ мінімуму.
Producer отримує підтвердження від 2 ISR → `acks=all` виконано → OK.

---

**Питання 10:**
Яка з характеристик НЕ є перевагою збільшення `linger.ms`?

A) Вищий throughput через менше мережевих запитів
B) Краща компресія через більший batch
C) Менша latency першого повідомлення
D) Менше навантаження на мережу при высокому навантаженні

**Відповідь:** C —
Збільшення `linger.ms` **збільшує** latency: перше повідомлення чекає accumulation.
Всі інші пункти (A, B, D) — справжні переваги батчинга.