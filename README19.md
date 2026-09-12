# Branch 19 — Production-like Demo

## Що демонструє ця гілка

Повна production-ready система, яка об'єднує всі паттерни з попередніх гілок:

| Паттерн | Звідки | Де застосовано |
|---------|--------|----------------|
| Saga Choreography | branch13 | order → payment → inventory → notification |
| Idempotent Producer | branch12 | order-service, payment-service |
| Manual Offset Commit | branch06 | inventory-service |
| Dead Letter Topic | branch09 | notification-service |
| SASL/PLAIN + ACL | branch18 | всі 4 сервіси |
| 3-broker cluster + `min.insync.replicas=2` | branch16 | kafka-1/2/3 |
| Avro + Schema Registry | branch11/13 | всі сервіси |
| Prometheus + Grafana | branch17 | consumer lag, DLT counter |

## Архітектура

```
POST /api/orders
      │
      ▼
order-service ──[19.orders.created]──► payment-service
  (idempotent                              │ OK → [19.payments.processed]
   producer,                               │ FAIL → [19.payments.failed] ──► OrderCancelled
   acks=all)                               ▼
                               inventory-service (manual commit)
                                           │ OK → [19.inventory.reserved] ──► OrderConfirmed
                                           │ FAIL → [19.inventory.failed]
                                                 │
                                                 └─► payment-service → [19.payments.refunded]
                                                 └─► order-service   → [19.orders.cancelled]

notification-service (read_committed, DLT) слухає всі топіки
```

## Порти

| Сервіс | Порт | Деталі |
|--------|------|--------|
| kafka-1 | 9122 | Broker 1 (KRaft, SASL_PLAINTEXT) |
| kafka-2 | 9123 | Broker 2 |
| kafka-3 | 9124 | Broker 3 |
| schema-registry | 8191 | Confluent Schema Registry |
| kafka-ui | 8192 | Веб-інтерфейс кластера |
| order-service | 8193 | Saga init, idempotent producer |
| payment-service | 8194 | Saga step 2, idempotent consumer |
| inventory-service | 8195 | Saga step 3, manual commit |
| notification-service | 8196 | Saga observer, DLT |
| prometheus | 9090 | Метрики |
| grafana | 3019 | Dashboards |

## SASL користувачі та ACL

| Користувач | Пароль | WRITE | READ |
|---|---|---|---|
| `admin` | `admin-secret` | все (super user) | все |
| `order-producer` | `order-secret` | `19.orders.created` | `19.payments.failed`, `19.inventory.*` |
| `payment-processor` | `payment-secret` | `19.payments.*` | `19.orders.created`, `19.inventory.failed` |
| `inventory-worker` | `inventory-secret` | `19.inventory.*` | `19.payments.processed` |
| `notification-consumer` | `notif-secret` | `19.orders.created.dlt` | всі `19.*` топіки |

## Топіки

Всі топіки: `partitions=3`, `replication-factor=3`, `min.insync.replicas=2`

```
19.orders.created
19.payments.processed
19.payments.failed
19.payments.refunded
19.inventory.reserved
19.inventory.failed
19.orders.confirmed
19.orders.cancelled
19.orders.created.dlt      ← DLT (partitions=1)
```

## Запуск

```bash
docker compose -f docker-compose-19.yml up --build
```

Kafka UI: http://localhost:8192  
Grafana:  http://localhost:3019 (admin/admin)

## Демонстрація

### 1. Happy path — успішне замовлення

```bash
curl -s -X POST http://localhost:8193/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-42", "itemCount": 3}' | jq
```

Логи (в порядку):
```
[SAGA-START]       order-service:  orderId=... idempotencyKey=order-created:...
[SAGA 1/5]         notification:   ORDER CREATED    orderId=...
[PAYMENT-SAGA]     payment:        APPROVED orderId=...
[SAGA 2/5]         notification:   PAYMENT APPROVED orderId=...
[INVENTORY-SAGA]   inventory:      RESERVED orderId=...  ← manual ack після публікації
[SAGA 3/5]         notification:   INVENTORY RESERVED orderId=...
[SAGA-COMPLETE]    order-service:  status=CONFIRMED
[SAGA DONE ✅]     notification:   ORDER CONFIRMED orderId=...
```

### 2. Компенсація — відмова платежу

```bash
# Наступний платіж провалиться
curl -X POST "http://localhost:8194/api/payments/fail-next?count=1"

# Замовлення → компенсація
curl -s -X POST http://localhost:8193/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-99"}' | jq
```

Логи компенсації:
```
[PAYMENT-SAGA]    FAILED → saga compensation triggered
[SAGA COMP]       PAYMENT FAILED
[SAGA-COMPENSATE] status=CANCELLED
[SAGA DONE ❌]    ORDER CANCELLED reason=Payment: ...
```

### 3. Компенсація — відсутність на складі

```bash
curl -X POST "http://localhost:8195/api/inventory/out-of-stock-next?count=1"
curl -s -X POST http://localhost:8193/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-77"}' | jq
```

Логи компенсації:
```
[INVENTORY-SAGA]  OUT OF STOCK → compensation triggered
[PAYMENT-COMPENSATE] Refunded orderId=...
[SAGA DONE ❌]    ORDER CANCELLED reason=Inventory: ...
```

### 4. Перегляд consumer lag в Grafana

```bash
# Симуляція lag: надіслати 20 замовлень швидко
for i in $(seq 1 20); do
  curl -s -X POST http://localhost:8193/api/orders \
    -H "Content-Type: application/json" \
    -d "{\"userId\": \"load-test-$i\"}" > /dev/null
done
```

Відкрити Grafana → дашборд **Kafka Observability — Branch 19**: видно зростання lag і відновлення.

### 5. Статус замовлення

```bash
curl http://localhost:8193/api/orders/{orderId}/status
```

### 6. Перегляд ACL

```bash
docker exec init-topics-b19 kafka-acls \
  --bootstrap-server kafka-1:9092 \
  --command-config /etc/kafka/secrets/admin.conf \
  --list
```

### 7. Падіння брокера (перевірка відмовостійкості)

```bash
# Зупинити один брокер — система продовжує працювати (min.isr=2, rf=3)
docker stop kafka-2

# Надіслати замовлення — має спрацювати
curl -s -X POST http://localhost:8193/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId": "resilience-test"}' | jq

# Зупинити другий брокер — producer отримає NotEnoughReplicasException
docker stop kafka-3
```

## Production checklist

- [x] `acks=all` + `min.insync.replicas=2` + `replication-factor=3`
- [x] `enable.idempotence=true` (order-service, payment-service)
- [x] Manual commit після успішної обробки (inventory-service)
- [x] Dead Letter Topic для некоректних повідомлень (notification-service)
- [x] Consumer lag alerting (Grafana dashboard)
- [x] SASL/PLAIN + ACL (принцип least privilege)
- [x] Avro + Schema Registry (строга типізація)
- [x] Saga choreography з compensation (розподілені транзакції)
- [x] `isolation.level=read_committed` (notification-service)
- [x] `AUTO_CREATE_TOPICS_ENABLE=false`

## Ключові відмінності від branch13 (Saga без prod-налаштувань)

| | branch13 | branch19 |
|---|---|---|
| Броукери | 1 | 3 |
| Безпека | немає | SASL + ACL |
| Replication | rf=1 | rf=3, min.isr=2 |
| Producer | звичайний | idempotent |
| Commit | auto | manual (inventory) |
| DLT | немає | є (notification) |
| Monitoring | немає | Prometheus + Grafana |

## Файли гілки

```
branch19_production/
├── kafka/
│   ├── kafka_server_jaas.conf    ← 5 SASL users
│   ├── admin.conf                ← admin client для CLI
│   └── init.sh                   ← створює 9 топіків + ACL для всіх сервісів
├── prometheus/prometheus.yml
├── grafana/
│   ├── provisioning/             ← автопровізія datasource + dashboard
│   └── dashboards/kafka-lag.json
├── order-service/                ← idempotent producer, saga init
├── payment-service/              ← idempotent consumer, saga step 2
├── inventory-service/            ← manual commit, saga step 3
│   └── config/KafkaConfig.kt    ← AckMode.MANUAL_IMMEDIATE
└── notification-service/
    └── config/KafkaConfig.kt    ← DefaultErrorHandler + DeadLetterPublishingRecoverer
```