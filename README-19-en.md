# Branch 19 — Production-like Demo

## 1. What we learn in this branch

- **Production-ready system** — combining all patterns from previous branches into a single stack.
- **Saga Choreography** (branch13) — 4 services, 8+ topics, compensating transactions.
- **Idempotent Producer** (branch12) — `enable.idempotence=true` in order-service and payment-service.
- **Manual Offset Commit** (branch06) — `AckMode.MANUAL_IMMEDIATE` in inventory-service.
- **Dead Letter Topic** (branch09) — `DefaultErrorHandler + DeadLetterPublishingRecoverer` in
  notification-service.
- **SASL/PLAIN + ACL** (branch18) — 5 unique users, each service has only the required permissions.
- **3-broker cluster** (branch16) — KRaft, RF=3, `min.insync.replicas=2`.
- **Avro + Schema Registry** (branch11/13) — all inter-service events are typed.
- **Prometheus + Grafana** (branch17) — consumer lag dashboard + DLT counter.

---

## 2. Changes compared to the previous branch (branch18)

- **3 Kafka brokers** instead of one (`kafka-b19-1`, `kafka-b19-2`, `kafka-b19-3`).
- **Restored** inventory-service (branch13) with manual commit (branch06).
- **Added** Schema Registry, Prometheus, Grafana.
- **SASL users**: 5 instead of 3 (+ `payment-processor`, `inventory-worker`).
- **9 Kafka topics** (8 business topics + 1 DLT) with RF=3, partitions=3.
- **init-topics** — a separate container that creates topics + ACLs (replaces acl-init).
- **Separate ports** for each service (8193–8196) to avoid conflicts.

---

## 3. Architecture

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
  listens to ALL 8 topics → logs each saga step
  on error → 19.orders.created.dlt (DLT)

Kafka Cluster: kafka-b19-1:9122, kafka-b19-2:9123, kafka-b19-3:9124
Schema Registry: 8191
Grafana: 3019
```

**Services and ports (from docker-compose-19.yml):**
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

## 4. Key Concepts

### Production Checklist

```
✓ acks=all + min.insync.replicas=2 + replication-factor=3
✓ enable.idempotence=true (order-service, payment-service)
✓ Manual commit after successful processing (inventory-service)
✓ Dead Letter Topic for malformed messages (notification-service)
✓ Consumer lag alerting (Grafana dashboard)
✓ SASL/PLAIN + ACL (principle of least privilege)
✓ Avro + Schema Registry (strict typing)
✓ Saga choreography with compensation (distributed transactions)
✓ isolation.level=read_committed (notification-service)
✓ AUTO_CREATE_TOPICS_ENABLE=false
```

### SASL Users and ACL

```
User                  Password            WRITE                    READ
admin                 admin-secret        everything (super user)  everything
order-producer        order-secret        19.orders.created        19.payments.failed, 19.inventory.*
payment-processor     payment-secret      19.payments.*            19.orders.created, 19.inventory.failed
inventory-worker      inventory-secret    19.inventory.*           19.payments.processed
notification-consumer notif-secret        19.orders.created.dlt    all 19.* topics
```

### Topics

```
All topics: partitions=3, replication-factor=3, min.insync.replicas=2

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

### branch13 vs branch19 Comparison

```
                     branch13          branch19
Brokers              1                 3 (KRaft)
Security             none              SASL/PLAIN + ACL
Replication          RF=1              RF=3, min.isr=2
Producer             standard          idempotent
Commit               auto              manual (inventory)
DLT                  none              yes (notification)
Monitoring           none              Prometheus + Grafana
Schema Registry      8090              8191
```

---

## 5. How to Run

```bash
docker compose -f docker-compose-19.yml up --build

# First run takes ~4 minutes (building 4 services + 3 brokers)
# Kafka UI:  http://localhost:8192
# Grafana:   http://localhost:3019 (admin/admin)
```

**Startup order:**
```
kafka-b19-1, kafka-b19-2, kafka-b19-3 → healthy
init-topics → creates 9 topics + ACLs → exits(0)
schema-registry-b19 → healthy
order-service, payment-service, inventory-service, notification-service → start
```

---

## 6. How to Test

### Scenario 1 — Happy Path

```bash
curl -s -X POST http://localhost:8193/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-42", "itemCount": 3}' | jq .

# notification-service logs (in order):
# [SAGA-START]       order-service:  orderId=... idempotencyKey=order-created:...
# [SAGA 1/5]         notification:   ORDER CREATED    orderId=...
# [PAYMENT-SAGA]     payment:        APPROVED orderId=...
# [SAGA 2/5]         notification:   PAYMENT APPROVED orderId=...
# [INVENTORY-SAGA]   inventory:      RESERVED orderId=...  ← manual ack after publish
# [SAGA 3/5]         notification:   INVENTORY RESERVED orderId=...
# [SAGA-COMPLETE]    order-service:  status=CONFIRMED
# [SAGA DONE ✅]     notification:   ORDER CONFIRMED orderId=...

# Check status
curl http://localhost:8193/api/orders/{orderId}/status
```

### Scenario 2 — Compensation: Payment Failure

```bash
curl -X POST "http://localhost:8194/api/payments/fail-next?count=1"

curl -s -X POST http://localhost:8193/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-99"}' | jq .

# Logs:
# [PAYMENT-SAGA]    FAILED → saga compensation triggered
# [SAGA COMP]       PAYMENT FAILED
# [SAGA-COMPENSATE] status=CANCELLED
# [SAGA DONE ❌]    ORDER CANCELLED reason=Payment: ...
```

### Scenario 3 — Compensation: Out of Stock

```bash
curl -X POST "http://localhost:8195/api/inventory/out-of-stock-next?count=1"

curl -s -X POST http://localhost:8193/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-77"}' | jq .

# Logs:
# [INVENTORY-SAGA]  OUT OF STOCK → compensation triggered
# [PAYMENT-COMPENSATE] Refunded orderId=...
# [SAGA DONE ❌]    ORDER CANCELLED reason=Inventory: ...
```

### Scenario 4 — Consumer Lag in Grafana

```bash
# Simulate lag: 20 orders quickly
for i in $(seq 1 20); do
  curl -s -X POST http://localhost:8193/api/orders \
    -H "Content-Type: application/json" \
    -d "{\"userId\": \"load-test-$i\"}" > /dev/null
done

# Grafana → dashboard "Kafka Observability — Branch 19"
# Observe lag growing and recovering
```

### Scenario 5 — Resilience: Broker Failure

```bash
# Stop one broker — the system continues working (min.isr=2, rf=3)
docker stop kafka-b19-2

curl -s -X POST http://localhost:8193/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId": "resilience-test"}' | jq .
# Success: ISR=[1,3] >= min.isr=2

# Stop a second broker — producer receives NotEnoughReplicasException
docker stop kafka-b19-3
# ISR=[1] < min.isr=2 → error
```

### Scenario 6 — View ACLs

```bash
docker exec init-topics-b19 kafka-acls \
  --bootstrap-server kafka-b19-1:9092 \
  --command-config /etc/kafka/secrets/admin.conf \
  --list
```

---

## 7. Deep Dive

### Why does inventory-service use manual commit?

```kotlin
// inventory-service/config/KafkaConfig.kt
factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE
```

inventory-service performs:
1. Checks item availability
2. Publishes `inventory.reserved` or `inventory.failed` to Kafka
3. **Only after successful publish** → `ack.acknowledge()`

If the service crashes between step 2 and step 3 → the message is re-read (at-least-once).
The idempotency key protects against double-processing.

### DLT in notification-service

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
            FixedBackOff(1000L, 2)  // 2 retries, then DLT
        )
    )
    return factory
}
```

### Grafana Dashboard — Branch 19

```
Dashboard "Kafka Observability — Branch 19":
  Consumer Lag (total)      ← total lag across 4 consumer groups
  DLT Messages              ← message count in 19.orders.created.dlt
  Saga Success Rate         ← confirmed / (confirmed + cancelled)
  Lag Over Time             ← per consumer group over time
  Active Brokers            ← 3 → 2 → 1 → 2 → 3
```

---

## 8. Project Structure

```
branch19_production/
├── kafka/
│   ├── kafka_server_jaas.conf  ← 5 SASL users
│   ├── admin.conf              ← admin client for CLI
│   └── init.sh                 ← creates 9 topics + ACLs for all services
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

## 9. What's Next

- **Branch 20** — Kafka Streams Advanced: KStream-KTable join, GlobalKTable, EOS.
- **Branch 23** — SSL/TLS: SASL_SSL = SASL + SSL encryption.

---

## 10. Lecture Slides

### Slide 1 — Production Checklist

```
Production Kafka Checklist:
  □ acks=all + min.insync.replicas ≥ 2
  □ enable.idempotence=true (if exactly-once on producer is needed)
  □ RF ≥ 3 for critical topics
  □ Manual commit + idempotent consumers
  □ DLT for handling poison messages
  □ Consumer lag alerting (Grafana/PagerDuty)
  □ SASL + ACL (least privilege)
  □ Schema Registry + Avro (breaking changes prevention)
  □ AUTO_CREATE_TOPICS=false
  □ Retention policy per topic
```

### Slide 2 — Pyramid of Features

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

## 11. Demo Script

```bash
#!/bin/bash
echo "=== Branch 19: Production-like Demo ==="

docker compose -f docker-compose-19.yml up --build -d
echo "Waiting 4 minutes..."
sleep 240

echo ""
echo "=== Scenario 1: Happy Path ==="
RESP=$(curl -s -X POST http://localhost:8193/api/orders \
  -H "Content-Type: application/json" -d '{"userId":"user-1","itemCount":2}')
echo "$RESP" | jq .
ORDER_ID=$(echo "$RESP" | jq -r '.orderId')
sleep 5
echo "Status: $(curl -s http://localhost:8193/api/orders/$ORDER_ID/status)"

echo ""
echo "=== Scenario 2: Payment Failure ==="
curl -s -X POST "http://localhost:8194/api/payments/fail-next?count=1"
RESP=$(curl -s -X POST http://localhost:8193/api/orders \
  -H "Content-Type: application/json" -d '{"userId":"user-2"}')
ORDER_ID=$(echo "$RESP" | jq -r '.orderId')
sleep 5
echo "Status: $(curl -s http://localhost:8193/api/orders/$ORDER_ID/status)"

echo ""
echo "=== Load Test: 20 orders → Grafana lag ==="
for i in $(seq 1 20); do
  curl -s -X POST http://localhost:8193/api/orders \
    -H "Content-Type: application/json" \
    -d "{\"userId\":\"load-$i\"}" > /dev/null
done
echo "Open Grafana: http://localhost:3019"

echo ""
echo "=== Resilience: stop kafka-b19-2 ==="
docker stop kafka-b19-2
sleep 10
RESP=$(curl -s -X POST http://localhost:8193/api/orders \
  -H "Content-Type: application/json" -d '{"userId":"resilience"}')
echo "With 2 brokers: $RESP" | jq .sent
docker start kafka-b19-2
```

---

## 12. Self-Check Questions

- Why does inventory-service use manual commit while payment-service does not?
- What prevents double-charging when retrying?
- How does the DLT in notification-service prevent a poison message from getting stuck?
- How many brokers can fail for the system to continue accepting orders?
- Why does notification-service use `isolation.level=read_committed`?
- What role does Schema Registry play in a system with 4 services and 8 topics?
- What happens if inventory-service crashes between reserving and committing the offset?
- Why does branch19 have 5 SASL users instead of 3 (as in branch18)?