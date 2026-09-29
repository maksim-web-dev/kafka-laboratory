# Branch 19 — Production-like Demo

## 1. Що вивчаємо у цій гілці

- **Production-ready система** — поєднання всіх паттернів з попередніх гілок у єдиний стек.
- **Saga Choreography** (branch13) — 4 сервіси, 8+ топіків, compensating transactions.
- **Idempotent Producer** (branch12) — `enable.idempotence=true` у order-service та payment-service.
- **Manual Offset Commit** (branch06) — `AckMode.MANUAL_IMMEDIATE` у inventory-service.
- **Dead Letter Topic** (branch09) — `DefaultErrorHandler + DeadLetterPublishingRecoverer` у
  notification-service.
- **SASL/PLAIN + ACL** (branch18) — 5 унікальних users, кожен сервіс має лише необхідні права.
- **3-broker cluster** (branch16) — KRaft, RF=3, `min.insync.replicas=2`.
- **Avro + Schema Registry** (branch11/13) — усі події між сервісами типізовані.
- **Prometheus + Grafana** (branch17) — consumer lag dashboard + DLT counter.

---

## 2. Зміни порівняно з попередньою гілкою (branch18)

- **3 Kafka брокери** замість одного (`kafka-b19-1`, `kafka-b19-2`, `kafka-b19-3`).
- **Повернуто** inventory-service (branch13) з manual commit (branch06).
- **Додано** Schema Registry, Prometheus, Grafana.
- **SASL users**: 5 замість 3 (+ `payment-processor`, `inventory-worker`).
- **9 Kafka топіків** (8 бізнес-топіків + 1 DLT) з RF=3, partitions=3.
- **init-topics** — окремий контейнер що створює топіки + ACL (замість acl-init).
- **Окремі порти** для кожного сервісу (8193-8196) щоб уникнути конфліктів.

---

## 3. Архітектура

```
POST /api/orders
      │
      ▼
order-service (idempotent, acks=all)
  ──[19.orders.created]──────────────────► payment-service (idempotent)
                                                 │
                                    OK → [19.payments.processed]
                                   FAIL → [19.payments.failed] ──► order-service
                                                 │                   (cancel)
                                                 ▼
                                          inventory-service (manual commit)
                                                 │
                                    OK → [19.inventory.reserved] ──► order-service
                                                                       (confirm)
                                   FAIL → [19.inventory.failed] ─── payment-service
                                                              └───── order-service

notification-service (read_committed, DLT)
  слухає ВСІ 8 топіків → logs кожен крок саги
  при помилці → 19.orders.created.dlt (DLT)

Kafka Cluster: kafka-b19-1:9122, kafka-b19-2:9123, kafka-b19-3:9124
Schema Registry: 8191
Grafana: 3019
```

**Сервіси та порти (з docker-compose-19.yml):**
- `kafka-b19-1` — 9122 (external)
- `kafka-b19-2` — 9123 (external)
- `kafka-b19-3` — 9124 (external)
- `schema-registry-b19` — 8191 → http://localhost:8191
- `kafka-ui-b19` — 8192 → http://localhost:8192
- `prometheus-b19` — 9090 → http://localhost:9090
- `grafana-b19` — 3019 → http://localhost:3019 (admin/admin)
- `order-service-b19` — 8193 → http://localhost:8193
- `payment-service-b19` — 8194 → http://localhost:8194
- `inventory-service-b19` — 8195 → http://localhost:8195
- `notification-service-b19` — 8196 → http://localhost:8196

---

## 4. Ключові концепції

### Production Checklist

```
✓ acks=all + min.insync.replicas=2 + replication-factor=3
✓ enable.idempotence=true (order-service, payment-service)
✓ Manual commit після успішної обробки (inventory-service)
✓ Dead Letter Topic для некоректних повідомлень (notification-service)
✓ Consumer lag alerting (Grafana dashboard)
✓ SASL/PLAIN + ACL (принцип least privilege)
✓ Avro + Schema Registry (строга типізація)
✓ Saga choreography з compensation (розподілені транзакції)
✓ isolation.level=read_committed (notification-service)
✓ AUTO_CREATE_TOPICS_ENABLE=false
```

### SASL Users та ACL

```
Користувач           Пароль              WRITE                    READ
admin                admin-secret        все (super user)         все
order-producer       order-secret        19.orders.created        19.payments.failed, 19.inventory.*
payment-processor    payment-secret      19.payments.*            19.orders.created, 19.inventory.failed
inventory-worker     inventory-secret    19.inventory.*           19.payments.processed
notification-consumer notif-secret       19.orders.created.dlt   всі 19.* топіки
```

### Топіки

```
Всі топіки: partitions=3, replication-factor=3, min.insync.replicas=2

19.orders.created           ← saga init
19.payments.processed       ← payment ok
19.payments.failed          ← payment fail (compensation)
19.payments.refunded        ← inventory fail → refund
19.inventory.reserved       ← inventory ok
19.inventory.failed         ← inventory fail (compensation)
19.orders.confirmed         ← saga complete
19.orders.cancelled         ← saga compensation complete
19.orders.created.dlt       ← DLT (partitions=1)
```

### Порівняння branch13 vs branch19

```
                     branch13          branch19
Брокери              1                 3 (KRaft)
Безпека              немає             SASL/PLAIN + ACL
Replication          RF=1              RF=3, min.isr=2
Producer             звичайний         idempotent
Commit               auto              manual (inventory)
DLT                  немає             є (notification)
Monitoring           немає             Prometheus + Grafana
Schema Registry      8090              8191
```

---

## 5. Як запустити

```bash
docker compose -f docker-compose-19.yml up --build

# Перший запуск займає ~4 хвилини (збірка 4 сервісів + 3 брокери)
# Kafka UI:  http://localhost:8192
# Grafana:   http://localhost:3019 (admin/admin)
```

**Порядок запуску:**
```
kafka-b19-1, kafka-b19-2, kafka-b19-3 → healthy
init-topics → створює 9 топіків + ACL → exits(0)
schema-registry-b19 → healthy
order-service, payment-service, inventory-service, notification-service → start
```

---

## 6. Як тестувати

### Сценарій 1 — Happy Path

```bash
curl -s -X POST http://localhost:8193/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-42", "itemCount": 3}' | jq .

# Логи notification-service (в порядку):
# [SAGA-START]       order-service:  orderId=... idempotencyKey=order-created:...
# [SAGA 1/5]         notification:   ORDER CREATED    orderId=...
# [PAYMENT-SAGA]     payment:        APPROVED orderId=...
# [SAGA 2/5]         notification:   PAYMENT APPROVED orderId=...
# [INVENTORY-SAGA]   inventory:      RESERVED orderId=...  ← manual ack після публікації
# [SAGA 3/5]         notification:   INVENTORY RESERVED orderId=...
# [SAGA-COMPLETE]    order-service:  status=CONFIRMED
# [SAGA DONE ✅]     notification:   ORDER CONFIRMED orderId=...

# Перевірити статус
curl http://localhost:8193/api/orders/{orderId}/status
```

### Сценарій 2 — Компенсація: відмова платежу

```bash
curl -X POST "http://localhost:8194/api/payments/fail-next?count=1"

curl -s -X POST http://localhost:8193/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-99"}' | jq .

# Логи:
# [PAYMENT-SAGA]    FAILED → saga compensation triggered
# [SAGA COMP]       PAYMENT FAILED
# [SAGA-COMPENSATE] status=CANCELLED
# [SAGA DONE ❌]    ORDER CANCELLED reason=Payment: ...
```

### Сценарій 3 — Компенсація: відсутність товару

```bash
curl -X POST "http://localhost:8195/api/inventory/out-of-stock-next?count=1"

curl -s -X POST http://localhost:8193/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-77"}' | jq .

# Логи:
# [INVENTORY-SAGA]  OUT OF STOCK → compensation triggered
# [PAYMENT-COMPENSATE] Refunded orderId=...
# [SAGA DONE ❌]    ORDER CANCELLED reason=Inventory: ...
```

### Сценарій 4 — Consumer lag у Grafana

```bash
# Симуляція lag: 20 замовлень швидко
for i in $(seq 1 20); do
  curl -s -X POST http://localhost:8193/api/orders \
    -H "Content-Type: application/json" \
    -d "{\"userId\": \"load-test-$i\"}" > /dev/null
done

# Grafana → dashboard "Kafka Observability — Branch 19"
# Видно зростання lag і відновлення
```

### Сценарій 5 — Resilience: падіння брокера

```bash
# Зупинити один брокер — система продовжує працювати (min.isr=2, rf=3)
docker stop kafka-b19-2

curl -s -X POST http://localhost:8193/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId": "resilience-test"}' | jq .
# Успіх: ISR=[1,3] >= min.isr=2

# Зупинити другий брокер — producer отримає NotEnoughReplicasException
docker stop kafka-b19-3
# ISR=[1] < min.isr=2 → помилка
```

### Сценарій 6 — Переглянути ACL

```bash
docker exec init-topics-b19 kafka-acls \
  --bootstrap-server kafka-b19-1:9092 \
  --command-config /etc/kafka/secrets/admin.conf \
  --list
```

---

## 7. Поглиблений розгляд

### Чому inventory-service використовує manual commit?

```kotlin
// inventory-service/config/KafkaConfig.kt
factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE
```

inventory-service виконує:
1. Перевіряє наявність товару
2. Публікує `inventory.reserved` або `inventory.failed` у Kafka
3. **Тільки після успішної публікації** → `ack.acknowledge()`

Якщо сервіс впаде між кроком 2 і 3 → повідомлення перечитається (at-least-once).
Idempotency key захищає від повторної обробки.

### DLT у notification-service

```kotlin
// notification-service/config/KafkaConfig.kt
@Bean
fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, Any> {
    val factory = ConcurrentKafkaListenerContainerFactory<String, Any>()
    factory.setCommonErrorHandler(
        DefaultErrorHandler(
            DeadLetterPublishingRecoverer(kafkaTemplate) { r, _ ->
                TopicPartition("${r.topic()}.dlt", -1)
            },
            FixedBackOff(1000L, 2)  // 2 retry, потім DLT
        )
    )
    return factory
}
```

### Grafana Dashboard — Branch 19

```
Дашборд "Kafka Observability — Branch 19":
  Consumer Lag (total)      ← сумарний lag по 4 consumer groups
  DLT Messages              ← кількість повідомлень у 19.orders.created.dlt
  Saga Success Rate         ← confirmed / (confirmed + cancelled)
  Lag Over Time             ← per consumer group у часі
  Active Brokers            ← 3 → 2 → 1 → 2 → 3
```

---

## 8. Структура проекту

```
branch19_production/
├── kafka/
│   ├── kafka_server_jaas.conf  ← 5 SASL users
│   ├── admin.conf              ← admin client для CLI
│   └── init.sh                 ← створює 9 топіків + ACL для всіх сервісів
├── prometheus/prometheus.yml
├── grafana/
│   ├── provisioning/           ← auto-provision datasource + dashboard
│   └── dashboards/kafka-lag.json
├── order-service/              ← idempotent producer, saga init, acks=all
├── payment-service/            ← idempotent consumer, saga step 2
├── inventory-service/          ← manual commit (AckMode.MANUAL_IMMEDIATE)
│   └── config/KafkaConfig.kt
└── notification-service/       ← read_committed, DLT handler
    └── config/KafkaConfig.kt   ← DefaultErrorHandler + DeadLetterPublishingRecoverer
```

---

## 9. Що далі

- **Branch 20** — Kafka Streams Advanced: KStream-KTable join, GlobalKTable, EOS.
- **Branch 23** — SSL/TLS: SASL_SSL = SASL + SSL шифрування.

---

## 10. Слайди для лекції

### Слайд 1 — Production Checklist

```
Чекліст Production Kafka:
  □ acks=all + min.insync.replicas ≥ 2
  □ enable.idempotence=true (якщо потрібно exactly-once на producer)
  □ RF ≥ 3 для критичних топіків
  □ Manual commit + idempotent consumers
  □ DLT для обробки poison messages
  □ Consumer lag alerting (Grafana/PagerDuty)
  □ SASL + ACL (least privilege)
  □ Schema Registry + Avro (breaking changes prevention)
  □ AUTO_CREATE_TOPICS=false
  □ Retention policy per topic
```

### Слайд 2 — Pyramid of Features

```
branch19 = base + security + HA + schema + observability

             [branch19 = all combined]
            /           |           \
     [branch18]   [branch16]   [branch17]
     SASL/ACL    3-broker       Monitoring
         |           |               |
     [branch13]  [branch12]   [branch09]
     Saga        Transactions  DLT
```

---

## 11. Демонстраційний сценарій

```bash
#!/bin/bash
echo "=== Branch 19: Production-like Demo ==="

docker compose -f docker-compose-19.yml up --build -d
echo "Очікуємо 4 хвилини..."
sleep 240

echo ""
echo "=== Сценарій 1: Happy Path ==="
RESP=$(curl -s -X POST http://localhost:8193/api/orders \
  -H "Content-Type: application/json" -d '{"userId":"user-1","itemCount":2}')
echo "$RESP" | jq .
ORDER_ID=$(echo "$RESP" | jq -r '.orderId')
sleep 5
echo "Status: $(curl -s http://localhost:8193/api/orders/$ORDER_ID/status)"

echo ""
echo "=== Сценарій 2: Payment Failure ==="
curl -s -X POST "http://localhost:8194/api/payments/fail-next?count=1"
RESP=$(curl -s -X POST http://localhost:8193/api/orders \
  -H "Content-Type: application/json" -d '{"userId":"user-2"}')
ORDER_ID=$(echo "$RESP" | jq -r '.orderId')
sleep 5
echo "Status: $(curl -s http://localhost:8193/api/orders/$ORDER_ID/status)"

echo ""
echo "=== Load Test: 20 замовлень → Grafana lag ==="
for i in $(seq 1 20); do
  curl -s -X POST http://localhost:8193/api/orders \
    -H "Content-Type: application/json" \
    -d "{\"userId\":\"load-$i\"}" > /dev/null
done
echo "Відкрити Grafana: http://localhost:3019"

echo ""
echo "=== Resilience: зупинити kafka-b19-2 ==="
docker stop kafka-b19-2
sleep 10
RESP=$(curl -s -X POST http://localhost:8193/api/orders \
  -H "Content-Type: application/json" -d '{"userId":"resilience"}')
echo "При 2 брокерах: $RESP" | jq .sent
docker start kafka-b19-2
```

---

## 12. Питання для самоперевірки

- Навіщо inventory-service використовує manual commit, а payment-service — ні?
- Що захищає від подвійного списання коштів при retry?
- Як DLT у notification-service запобігає "застряганню" poison message?
- Скільки брокерів може впасти щоб система продовжувала приймати замовлення?
- Чому notification-service використовує `isolation.level=read_committed`?
- Яка роль Schema Registry у системі з 4 сервісами та 8 топіками?
- Що відбудеться якщо inventory-service впаде між резервуванням і commit offset?
- Чому в branch19 5 SASL users замість 3 (як у branch18)?