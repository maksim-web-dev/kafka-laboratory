# Branch 04 — Consumer Groups

## Що змінилося порівняно з branch03

| Аспект | branch03 | branch04 |
|--------|----------|----------|
| Кількість notification-service | 1 | **3 екземпляри** |
| Partition assignment | 1 consumer читає всі 3 партиції | **кожен читає 1 партицію** |
| Assignment strategy | (не вказана, default) | **`RangeAssignor`** (явно) |
| Static membership | відсутнє | **`group.instance.id`** для кожного інстансу |
| `INSTANCE_ID` env var | відсутній | `notification-service-1/2/3` |
| Лог виводить | key + partition | key + partition + **instanceId** |
| `[REBALANCE]` логи | відсутні | **`ASSIGNED` / `REVOKED`** при rebalance |
| Нові endpoints | — | `GET /api/notifications/partitions` |
| `/count` відповідь | без instanceId | **з `instanceId`** |
| Container names | `*-b03` | `notification-service-{1,2,3}-b04` |
| Зовнішні порти | 9096, 8086, 8093, 8094 | **9098, 8088, 8095, 8096/8097/8098** |

---

## Архітектура

```
order-service-b04 :8095
        │
        ▼
Apache Kafka (3 partitions: orders.created)
   ┌────┴───────────┬────────────────┐
   │                │                │
partition-0     partition-1     partition-2
   │                │                │
   ▼                ▼                ▼
notification     notification     notification
service-1        service-2        service-3
:8096            :8097            :8098
  └──────────────┴────────────────┘
          notification-service-group
```

### Consumer Group правило

> Кожна партиція може читатися **тільки одним** consumer-ом у групі одночасно.

З 3 партиціями і 3 consumer-ами:
- `notification-service-1` → `orders.created[0]`
- `notification-service-2` → `orders.created[1]`
- `notification-service-3` → `orders.created[2]`

Якщо зупинити `notification-service-3` → **rebalance**:
- `notification-service-1` → `orders.created[0]`
- `notification-service-2` → `orders.created[1]` + `orders.created[2]`

---

## Ключові концепції

### RangeAssignor vs RoundRobinAssignor

```
Топіки: orders.created (3 partitions), orders.cancelled (1 partition)
Consumer-и: C1, C2, C3

RangeAssignor (default):
  Сортує партиції, ділить рівномірно по consumer-ах для кожного топіку:
  C1 → orders.created[0], orders.cancelled[0]
  C2 → orders.created[1]
  C3 → orders.created[2]
  (C1 отримує більше — "нерівномірний" розподіл при непропорційній кількості)

RoundRobinAssignor:
  Об'єднує всі партиції всіх топіків, роздає по черзі:
  C1 → orders.created[0], orders.cancelled[0]  ← (ще зайве!)
  C2 → orders.created[1]
  C3 → orders.created[2]
  (краще при багатьох топіках з різною кількістю партицій)
```

### Static Group Membership (group.instance.id)

```yaml
# application.yml
group.instance.id: notification-service-1
```

**Без static membership (default):**
1. Consumer зупиняється → після `session.timeout.ms` (45с) вважається мертвим
2. Kafka починає rebalance — всі consumer-и зупиняють читання
3. Consumer перезапускається → отримує можливо інші партиції

**Зі static membership:**
1. Consumer зупиняється → Kafka чекає `session.timeout.ms`
2. Consumer повертається за той самий час → отримує назад ті ж партиції без rebalance
3. Це важливо для k8s pods із rolling deploy

### Rebalance Protocol

```
[ASSIGNED] Instance [notification-service-1] ASSIGNED: [orders.cancelled[0], orders.created[0]]
[ASSIGNED] Instance [notification-service-2] ASSIGNED: [orders.created[1]]
[ASSIGNED] Instance [notification-service-3] ASSIGNED: [orders.created[2]]

# Після зупинки notification-service-3:
[REBALANCE] Instance [notification-service-3] REVOKED:  [orders.created[2]]
[REBALANCE] Instance [notification-service-1] REVOKED:  [orders.cancelled[0], orders.created[0]]
[REBALANCE] Instance [notification-service-2] REVOKED:  [orders.created[1]]
[ASSIGNED]  Instance [notification-service-1] ASSIGNED: [orders.cancelled[0], orders.created[0]]
[ASSIGNED]  Instance [notification-service-2] ASSIGNED: [orders.created[1], orders.created[2]]
```

---

## Як запустити

```bash
# Зупинити branch03 якщо запущено
docker stop kafka kafka-ui b03-order-service b03-notification-service 2>/dev/null || true

# Запустити branch04 (чисті volumes)
docker compose -f docker-compose-04.yml down -v 2>/dev/null || true
docker compose -f docker-compose-04.yml up --build
```

Або поруч з branch03 (порти не перетинаються):

```bash
docker compose -f docker-compose-04.yml up --build
```

Перевірити всі 6 контейнерів:

```bash
docker compose -f docker-compose-04.yml ps
# kafka, kafka-ui, order-service-b04
# notification-service-1-b04, notification-service-2-b04, notification-service-3-b04
```

---

## Як протестувати

### 1. Перевірити призначені партиції кожного інстансу

```bash
# Кожен інстанс повинен мати свою партицію
curl -s http://localhost:8096/api/notifications/partitions | jq
curl -s http://localhost:8097/api/notifications/partitions | jq
curl -s http://localhost:8098/api/notifications/partitions | jq
```

Очікувані відповіді:

```json
{"instanceId":"notification-service-1","assignedPartitions":["orders.cancelled[0]","orders.created[0]"],"count":2}
{"instanceId":"notification-service-2","assignedPartitions":["orders.created[1]"],"count":1}
{"instanceId":"notification-service-3","assignedPartitions":["orders.created[2]"],"count":1}
```

> `notification-service-1` отримує більше через `RangeAssignor` — `orders.cancelled` (1 partition) теж треба комусь призначити.

### 2. Надіслати повідомлення та подивитися хто обробив

```bash
# 6 замовлень від різних users → розподіл по 3 партиціях
for user in user-01 user-02 user-03 user-04 user-05 user-06; do
  curl -s -X POST http://localhost:8095/api/orders \
    -H "Content-Type: application/json" \
    -d "{\"userId\":\"$user\",\"product\":\"Book\",\"quantity\":1,\"totalAmount\":20.00}" \
    | jq -r '"Order for \(.userId) → orderId: \(.orderId)"'
done
```

Дивимося в логи — кожен інстанс обробив свою партицію:

```bash
docker logs notification-service-1-b04 --tail=20
docker logs notification-service-2-b04 --tail=20
docker logs notification-service-3-b04 --tail=20
```

Очікуваний лог `notification-service-2`:
```
╔══════════════════════════════════════════╗
║  ORDER CREATED  #2
║  Instance  : notification-service-2
║  Key: user-03  →  Partition: 1  Offset: 2
║  Order ID  : ...
║  User ID   : user-03
╚══════════════════════════════════════════╝
```

### 3. Демонструємо rebalance — зупиняємо один інстанс

```bash
# Зупиняємо третій інстанс
docker stop notification-service-3-b04
```

Чекаємо ~10-15 секунд (session.timeout.ms = 45с, але heartbeat timeout настає раніше).

```bash
# Перевіряємо перерозподіл
curl -s http://localhost:8096/api/notifications/partitions | jq
curl -s http://localhost:8097/api/notifications/partitions | jq
```

Очікуваний результат після rebalance:

```json
{"instanceId":"notification-service-1","assignedPartitions":["orders.cancelled[0]","orders.created[0]"],"count":2}
{"instanceId":"notification-service-2","assignedPartitions":["orders.created[1]","orders.created[2]"],"count":2}
```

`notification-service-2` тепер читає 2 партиції.

### 4. Відновлення — запускаємо назад

```bash
docker start notification-service-3-b04
```

Логи покажуть rebalance і повернення до рівномірного розподілу.

### 5. CLI моніторинг consumer group

```bash
# Реальний розподіл партицій + lag
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group notification-service-group
```

Очікуваний вивід (3 active members):
```
GROUP                       TOPIC              PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID                              HOST
notification-service-group  04.orders.created  0          6               6               0    notification-service-1-...  /172.x.x.x
notification-service-group  04.orders.created  1          4               4               0    notification-service-2-...  /172.x.x.x
notification-service-group  04.orders.created  2          5               5               0    notification-service-3-...  /172.x.x.x
```

### 6. Kafka UI

Відкрити: [http://localhost:8088](http://localhost:8088)

- `Consumer Groups` → `notification-service-group` → **Members** вкладка
- Видно 3 member-и, кожен з призначеними партиціями
- Після зупинки одного → тільки 2 members, один читає 2 партиції

---

## Як це працює всередині

### application.yml (notification-service) — що змінилося

```yaml
# branch03 (було):
spring.json.type.mapping: "..."
# (без assignment strategy)

# branch04 (стало):
partition.assignment.strategy: org.apache.kafka.clients.consumer.RangeAssignor
# group.instance.id НЕ використовується — пояснення нижче

instance:
  id: ${INSTANCE_ID:notification-service-1}
```

> **Чому без `group.instance.id`?**
> Spring Kafka створює окремий Kafka consumer для кожного `@KafkaListener` методу.
> Якщо в одному сервісі є 2 listener-и (`orders.created` + `orders.cancelled`),
> то 2 consumer-и отримають однаковий `group.instance.id` → Kafka відхиляє другий
> з `FencedInstanceIdException`. Щоб використати static membership, треба або
> мати лише один `@KafkaListener`, або конфігурувати окремі `ContainerFactory`
> з різними `group.instance.id` — це тема branch08.

### OrderEventListener.kt — нове

```kotlin
// Implements ConsumerSeekAware — отримує callbacks при rebalance
class OrderEventListener(
    @Value("\${instance.id}") private val instanceId: String
) : ConsumerSeekAware {

    override fun onPartitionsAssigned(assignments, callback) {
        log.info("[REBALANCE] Instance [{}] ASSIGNED: {}", instanceId, ...)
    }

    override fun onPartitionsRevoked(partitions) {
        log.info("[REBALANCE] Instance [{}] REVOKED:  {}", instanceId, ...)
    }
}
```

`ConsumerSeekAware` — Spring Kafka-інтерфейс для відстеження partition assignment.
Методи викликаються автоматично при кожному rebalance.

### NotificationController.kt — нове

```kotlin
@GetMapping("/partitions")
fun assignedPartitions(): Map<String, Any> {
    val parts = listener.getAssignedPartitions()
        .map { "${it.topic()}[${it.partition()}]" }.sorted()
    return mapOf("instanceId" to instanceId, "assignedPartitions" to parts, ...)
}
```

### docker-compose.yml — нове

Три окремі сервіси замість одного, з унікальним `INSTANCE_ID`:

```yaml
notification-service-1:
  environment:
    - INSTANCE_ID=notification-service-1
notification-service-2:
  environment:
    - INSTANCE_ID=notification-service-2
notification-service-3:
  environment:
    - INSTANCE_ID=notification-service-3
```

---

## Структура проєкту (зміни відносно branch03)

```
kafka-laboratory/
├── docker-compose-04.yml                      ← 3 notification-service-{1,2,3}-b04
├── branch04_customer_groups/
│   ├── notification-service/
│   │   └── src/main/kotlin/com/kafkalab/notification/
│   │       ├── controller/NotificationController.kt  ← instanceId + GET /partitions
│   │       └── listener/OrderEventListener.kt        ← ConsumerSeekAware + instanceId в логах
│   │   └── src/main/resources/
│   │       └── application.yml                       ← assignment.strategy, INSTANCE_ID
│   └── order-service/
│       └── src/main/kotlin/com/kafkalab/order/
│           ├── config/KafkaTopicConfig.kt            ← топіки 04.orders.*
│           └── service/OrderService.kt               ← публікує в 04.orders.*
└── README04.md
```

---

## Ключові концепції цієї гілки

| Концепція | Що демонструє приклад |
|-----------|----------------------|
| **Consumer Group** | 3 інстанси читають один топік незалежно — кожен отримує свою партицію |
| **1 partition = 1 consumer** | Kafka гарантує: одна партиція → один consumer у групі одночасно |
| **RangeAssignor** | Сортує партиції, рівномірно розподіляє: C1→[0], C2→[1], C3→[2] |
| **Rebalance** | При відключенні C3 → Kafka перерозподіляє її партицію на C2 |
| **Static Membership** | `group.instance.id` → consumer повертається без full rebalance |
| **Consumer lag** | `kafka-consumer-groups --describe` показує відставання кожного інстансу |
| **Parallelism** | 3 consumer-и = паралельна обробка партицій → 3x throughput |

---

## Що далі — branch05

У наступній гілці вивчаємо **Multiple Consumer Groups**:
- Два незалежних сервіси читають один і той самий топік
- `notification-service-group` та `analytics-service-group` — кожна група має свій offset
- Зупинити analytics-service, опублікувати 10 подій → analytics дочитає пропущене
- Демонстрація: "replay" подій для нової consumer group