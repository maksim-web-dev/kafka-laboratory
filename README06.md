# Branch 06 — Offsets & Commits

## Що змінилося порівняно з branch05

| Аспект | branch05 | branch06 |
|--------|----------|----------|
| Commit mode | auto (`enable.auto.commit=true`) | **manual** (`enable-auto-commit=false`) |
| AckMode | — | **`MANUAL_IMMEDIATE`** |
| `@KafkaListener` signature | `ConsumerRecord` | `ConsumerRecord` + **`Acknowledgment ack`** |
| Симуляція збою | — | **`POST /api/notifications/simulate-failure/{count}`** |
| Відповідь `/count` | — | + поле **`redelivered`** |
| Сервіси | order + notification + analytics | **order + notification** (без analytics) |
| Порти | 9100/8090/8101/8102/8103 | **9102/8104/8105/8106** |

---

## Ключові концепції

### Auto-commit (до branch06)

```
Consumer отримує batch → Spring автоматично фіксує offset через кожні 5 сек
Якщо сервіс впаде між отриманням і обробкою — повідомлення ВТРАЧЕНЕ (at-most-once)
```

### Manual commit (branch06)

```
Consumer отримує повідомлення → обробляє → явно викликає ack.acknowledge()
Якщо сервіс впаде до acknowledge() → повідомлення буде перечитано (at-least-once)
```

### At-least-once delivery

> Повідомлення **гарантовано** буде оброблено хоча б один раз.
> При збої — перечитання з незафіксованого offset → можлива **дублікатна обробка**.

```
offset 5: OrderCreated{...}
Consumer: отримав → почав обробляти → crash перед ack.acknowledge()

При рестарті: Kafka бачить committed offset = 4
Consumer: знову отримує offset 5 → обробляє вдруге → ack.acknowledge()
```

### nack(sleepMillis)

`ack.nack(2000L)` — відхилити повідомлення та затримати повторну доставку на 2 секунди:

```
Consumer: отримав offset 5 → failNextN > 0 → ack.nack(2000L)
Через 2 секунди: offset 5 доставляється знову
(offset не фіксується — наступні повідомлення в партиції не читаються)
```

---

## Архітектура branch06

```
Order Service  →  orders.created (3 partitions)  →  notification-service-group
               →  orders.cancelled (1 partition)  →  notification-service-group

notification-service (manual commit):
  handleOrderCreated(record, ack):
    if failNextN > 0: ack.nack(2000L)  ← симуляція збою
    else: process → ack.acknowledge()  ← фіксуємо offset
```

---

## application.yml — зміни

```yaml
spring:
  kafka:
    listener:
      ack-mode: manual_immediate   # ← NEW: Spring не фіксує offset автоматично
    consumer:
      enable-auto-commit: false    # ← NEW: вимикаємо auto-commit на рівні Kafka
```

`MANUAL_IMMEDIATE` — `ack.acknowledge()` фіксує offset негайно (не накопичує).

---

## Як запустити

```bash
# Зупинити branch05
docker stop kafka kafka-ui order-service-b05 \
  notification-service-b05 analytics-service-b05 2>/dev/null || true

# Запустити branch06
docker compose -f docker-compose-06.yml down -v 2>/dev/null || true
docker compose -f docker-compose-06.yml up --build
```

Перевірити 4 контейнери:

```bash
docker compose -f docker-compose-06.yml ps
# kafka, kafka-ui, order-service-b06, notification-service-b06
```

---

## Як протестувати

### 1. Базова перевірка — manual commit працює

```bash
# Надіслати 3 замовлення
for i in 1 2 3; do
  curl -s -X POST http://localhost:8105/api/orders \
    -H "Content-Type: application/json" \
    -d "{\"userId\":\"user-0$i\",\"product\":\"Book\",\"quantity\":1,\"totalAmount\":25.00}" \
    | jq -r '.orderId'
done

# Перевірити лічильник
curl -s http://localhost:8106/api/notifications/count | jq
# {"instanceId":"notification-service-1","orders_created":3,"orders_cancelled":0,"redelivered":0,"total":3}
```

### 2. Головний демо-сценарій: симуляція збою + at-least-once

```bash
# Крок 1: Активувати збій для наступних 2 повідомлень
curl -s -X POST http://localhost:8106/api/notifications/simulate-failure/2 | jq
# {"instanceId":"notification-service-1","scheduledNacks":2,"message":"Next 2 message(s) will be nacked and redelivered after 2s"}

# Крок 2: Надіслати замовлення
curl -s -X POST http://localhost:8105/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-fail","product":"Doomed Order","quantity":1,"totalAmount":99.00}' \
  | jq -r '.orderId'

# У логах notification-service побачимо:
# [SIMULATED FAILURE] nacking partition=X offset=Y — redelivery in 2s
# (через 2 сек)
# [SIMULATED FAILURE] nacking partition=X offset=Y — redelivery in 2s
# (через 2 сек — тепер failNextN=0, обробляємо нормально)
# ╔══ ORDER CREATED #1 ...

# Крок 3: Перевірити лічильник — orders_created=1, redelivered=2
curl -s http://localhost:8106/api/notifications/count | jq
# {"orders_created":1,"redelivered":2,...}
```

### 3. Перевірити offset через CLI

```bash
# Під час збою (після nack, до успішної обробки) — LAG > 0
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group notification-service-group

# Після успішної обробки — LAG = 0 для всіх партицій
```

### 4. Kafka UI

Відкрити: [http://localhost:8104](http://localhost:8104)

- `Consumer Groups` → `notification-service-group`
- Спостерігати як LAG змінюється під час симуляції збою

---

## Різниця: auto vs manual commit

| | Auto commit | Manual commit |
|---|---|---|
| Коли фіксується | кожні N мс незалежно від обробки | після `ack.acknowledge()` |
| Збій під час обробки | повідомлення **втрачено** (at-most-once) | повідомлення **перечитується** (at-least-once) |
| Дублікати | немає | **можливі** при повторній доставці |
| Складність | просто | потребує idempotent обробку |

---

## Структура змін відносно branch05

```
kafka-laboratory/
├── docker-compose-06.yml                   ← 4 сервіси (без analytics), нові порти b06
├── branch06_offsets_commits/
│   ├── notification-service/
│   │   └── src/main/resources/application.yml ← ack-mode: manual_immediate, enable-auto-commit: false
│   │   └── src/main/kotlin/.../listener/
│   │       └── OrderEventListener.kt           ← +Acknowledgment, +failNextN, ack.nack()/ack.acknowledge()
│   │   └── src/main/kotlin/.../controller/
│   │       └── NotificationController.kt       ← +simulate-failure endpoint, +redelivered у /count
│   └── order-service/                          ← топіки 06.orders.*
└── README06.md
```

---

## Ключові концепції цієї гілки

| Концепція | Що демонструє |
|-----------|---------------|
| **Manual commit** | `enable-auto-commit=false` + `ack-mode=MANUAL_IMMEDIATE` |
| **`ack.acknowledge()`** | Явна фіксація offset після успішної обробки |
| **`ack.nack(millis)`** | Відхилення + повторна доставка через N мс |
| **At-least-once** | Збій до acknowledge → перечитування з незафіксованого offset |
| **At-most-once** | Auto-commit → можлива втрата при збої між commit і обробкою |
| **Idempotency** | При at-least-once обробник повинен бути ідемпотентним |

---

## Що далі — branch07

У наступній гілці вивчаємо **Dead Letter Queue (DLQ)**:
- Повідомлення, які не вдалося обробити після N спроб → `orders.created.DLT`
- `DeadLetterPublishingRecoverer` + `DefaultErrorHandler`
- Моніторинг та повторна обробка DLQ