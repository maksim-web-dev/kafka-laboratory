# Branch 16 — Cluster & Replication

## 1. Що вивчаємо у цій гілці

- **Multi-broker cluster** — 3 Kafka брокери в одному Docker Compose (KRaft mode, без ZooKeeper).
- **Replication Factor (RF)** — скільки копій кожної партиції зберігається в кластері.
- **ISR (In-Sync Replicas)** — підмножина реплік, що синхронізовані з лідером у реальному часі.
- **Leader Election** — при падінні брокера Kafka автоматично обирає нового лідера з ISR.
- **`min.insync.replicas`** — мінімум синхронізованих реплік для прийняття запису з `acks=all`.
- **`NotEnoughReplicasException`** — що відбувається, коли ISR < min.insync.replicas.
- **4 демо-сценарії**: нормальна робота, падіння одного брокера, падіння двох, відновлення.

---

## 2. Зміни порівняно з попередньою гілкою (branch15)

- **Замість одного** kafka-брокера тепер **3 broker/controller** вузли (kafka-1, kafka-2, kafka-3).
- **Повернуто** notification-service як простий consumer.
- **Видалено** Kafka Connect, PostgreSQL, Elasticsearch — фокус на infrastructure.
- **Топік** `16.orders.created`: partitions=3, replication-factor=3, min.insync.replicas=2.
- **order-service** отримав `/api/cluster/brokers` та `/api/cluster/topic-info` endpoints.
- **bootstrap-servers**: тепер містить всі 3 брокери через кому.

---

## 3. Архітектура

```
order-service (acks=all)
  bootstrap: kafka-1:9092,kafka-2:9092,kafka-3:9092
      │
      ▼
┌──────────────────────────────────────────┐
│    Kafka Cluster (KRaft, 3 nodes)        │
│                                          │
│  kafka-1 (broker + controller) [leader?] │
│  kafka-2 (broker + controller)           │
│  kafka-3 (broker + controller)           │
│                                          │
│  topic: 16.orders.created                │
│    partition-0: leader=1, ISR=[1,2,3]    │
│    partition-1: leader=2, ISR=[1,2,3]    │
│    partition-2: leader=3, ISR=[1,2,3]    │
└──────────────────────────────────────────┘
      │
      ▼
notification-service (consumer group)
```

**Сервіси та порти (з docker-compose-16.yml):**
- `kafka-1` — 9092 (external)
- `kafka-2` — 9093 (external)
- `kafka-3` — 9094 (external)
- `kafka-ui` — 8080 → http://localhost:8080
- `order-service-b16` — 8081 → http://localhost:8081
- `notification-service-b16` — 8082 → http://localhost:8082

---

## 4. Ключові концепції

### Replication Factor та ISR

```
Partition 0 з RF=3:
  Leader:   broker-1  ← всі reads/writes ідуть до лідера
  Replicas: [1, 2, 3] ← список всіх реплік
  ISR:      [1, 2, 3] ← In-Sync: ті що не відстають > replica.lag.time.max.ms

Фолловери asynchronously тягнуть дані від лідера.
Якщо фолловер відстає → виключається з ISR.
```

### acks=all + min.insync.replicas

```
acks=all → producer чекає підтвердження від УСІХ ISR (не лише лідера)

min.insync.replicas=2 → мінімальна кількість ISR для успішного запису

Комбінація:
  ISR=[1,2,3]: запис ok (3 >= 2) ✓
  ISR=[1,3]:   запис ok (2 >= 2) ✓ (один брокер може впасти)
  ISR=[1]:     NotEnoughReplicasException ✗ (1 < 2)
```

### Leader Election

```
kafka-2 (лідер partition-1) падає:
  1. Kafka controller виявляє: kafka-2 не надсилає heartbeats
  2. Controller вибирає нового лідера з ISR partition-1
     (наприклад, kafka-3 стає лідером partition-1)
  3. Оновлюється metadata — клієнти отримують нового лідера
  4. Час failover: ~10-30 секунд (залежить від session.timeout)

Важливо: лідером стає тільки нода з ISR (не будь-яка репліка!)
```

### KRaft: 3 broker + controller у кожному вузлі

```yaml
# kafka-1 environment
KAFKA_PROCESS_ROLES: broker,controller
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093
```

Кожен вузол є і broker (обслуговує клієнтів) і controller (KRaft Raft-based metadata).
Кворум: 2 з 3 контролерів потрібно для прийняття metadata рішень.

### Рекомендації по RF

```
Середовище    │ RF │ min.insync.replicas │ Витримує
──────────────┼────┼─────────────────────┼──────────────────
Розробка      │  1 │ 1                   │ Нічого (no HA)
Staging       │  2 │ 1                   │ 1 broker failure
Production    │  3 │ 2                   │ 1 broker failure + write
```

---

## 5. Як запустити

```bash
# Запустити 3-broker кластер
docker compose -f docker-compose-16.yml up --build

# Kafka UI: http://localhost:8080
```

**Перевірка кластера:**
```bash
# Стан брокерів через order-service API
curl http://localhost:8081/api/cluster/brokers | jq .
# [{"brokerId":1,...}, {"brokerId":2,...}, {"brokerId":3,...}]

# Стан реплікації
curl http://localhost:8081/api/cluster/topic-info | jq .
# partition-0: leader=1, isr=[1,2,3], underReplicated=false
```

---

## 6. Як тестувати

### Демо 1 — Нормальна робота (3/3 ISR)

```bash
# ISR повний: [1, 2, 3] для кожної партиції
curl http://localhost:8081/api/cluster/topic-info | jq .

# Відправити замовлення — всі успішно
curl -X POST "http://localhost:8081/api/orders/batch?users=alice,bob&count=5"
# {"requested":5,"sent":5,"failed":0}

# Kafka UI: http://localhost:8080 → Topics → 16.orders.created → Partitions
```

### Демо 2 — Падіння одного брокера (2/3 ISR)

```bash
# Зупинити kafka-2
docker compose -f docker-compose-16.yml stop kafka-2

# ISR: [1, 3] — under-replicated, але 2 >= min.insync.replicas=2
curl http://localhost:8081/api/cluster/topic-info | jq .
# underReplicated: true, але leader election відбувся

# Замовлення ПРОДОВЖУЮТЬ надходити
curl -X POST "http://localhost:8081/api/orders/batch?users=alice,bob&count=5"
# {"requested":5,"sent":5,"failed":0}
```

### Демо 3 — Падіння двох брокерів (1/3 ISR < min.insync.replicas)

```bash
# Зупинити kafka-3 (kafka-2 вже зупинений)
docker compose -f docker-compose-16.yml stop kafka-3

# ISR: [1] — лише 1 < min.insync.replicas=2
curl http://localhost:8081/api/cluster/topic-info | jq .

# ПОМИЛКА! NotEnoughReplicasException
curl -X POST "http://localhost:8081/api/orders/batch?users=alice,bob&count=5"
# {"requested":5,"sent":0,"failed":5}
# Лог: NotEnoughReplicasException: fewer in-sync replicas than required
```

### Демо 4 — Відновлення

```bash
# Запустити обидва брокери
docker compose -f docker-compose-16.yml start kafka-2 kafka-3

# Чекаємо ~30 сек на sync
sleep 30
curl http://localhost:8081/api/cluster/topic-info | jq .
# ISR=[1,2,3], underReplicated=false

# Виробництво відновлюється
curl -X POST "http://localhost:8081/api/orders/batch?users=alice,bob&count=5"
# {"requested":5,"sent":5,"failed":0}
```

---

## 7. Поглиблений розгляд

### Як Kafka вирішує, хто лідер?

```
При старті кластера:
  Controller призначає "preferred leader" для кожної партиції
  Preferred leader = перша репліка у списку (зазвичай broker-id % num_brokers)

При падінні лідера:
  Controller вибирає першу репліку з ISR (не preferred leader!)
  Якщо ISR порожній → unclean.leader.election.enable=false → не обирається
                     → unclean.leader.election.enable=true  → обирається з-під ISR
                       (ризик втрати даних!)

"Leader rebalance" (auto.leader.rebalance.enable=true):
  Після відновлення — preferred leader автоматично повертає собі роль
```

### Чому bootstrap-servers містить всі 3 брокери?

```yaml
SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka-1:9092,kafka-2:9092,kafka-3:9092
```

Bootstrap servers — лише для початкового підключення та отримання metadata.
Якщо kafka-1 недоступний, клієнт спробує kafka-2, потім kafka-3.
Після отримання metadata клієнт підключається безпосередньо до лідерів партицій.

### Internal topics RF

```yaml
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
```

`__consumer_offsets` та `__transaction_state` також мають RF=3.
Без цього при падінні брокера consumer group coordination може зламатись.

---

## 8. Структура проекту

```
branch16_cluster_replication/
├── order-service/
│   ├── controller/
│   │   ├── OrderController.kt     ← POST /api/orders, POST /api/orders/batch
│   │   └── ClusterController.kt   ← GET /api/cluster/brokers, GET /api/cluster/topic-info
│   ├── service/
│   │   ├── OrderService.kt
│   │   └── ClusterInfoService.kt  ← AdminClient для metadata
│   └── config/KafkaTopicConfig.kt ← topic з RF=3, min.insync.replicas=2
└── notification-service/
    └── listener/OrderEventListener.kt
```

---

## 9. Що далі

- **Branch 17** — Observability: Prometheus + Grafana + kafka-exporter; моніторинг consumer lag.
- **Branch 19** — Production-like: 3-broker cluster + SASL/ACL + Schema Registry + Saga.

---

## 10. Слайди для лекції

### Слайд 1 — Replication Factor

```
Partition 0, RF=3:

  Broker-1 [LEADER]  ◄── Producer writes here
      │
      ├──► Broker-2 [FOLLOWER]  (async replication)
      └──► Broker-3 [FOLLOWER]  (async replication)

ISR = {1, 2, 3}  ← всі в синхронізації
```

### Слайд 2 — min.insync.replicas

```
acks=all + min.insync.replicas=2:

3 brokers up  → ISR=[1,2,3] → write ok  ✓
1 broker down → ISR=[1,3]   → write ok  ✓ (2 >= 2)
2 brokers down → ISR=[1]    → EXCEPTION ✗ (1 < 2)

Trade-off: durability vs availability
  min.isr=1: більше availability, менше durability
  min.isr=3: більше durability, менше availability
```

### Слайд 3 — Leader Election

```
kafka-2 (leader partition-1) ✗ FALLS

           KRaft Controller detects failure
                     │
                     ▼
           New leader from ISR: kafka-3
                     │
              Clients get new metadata
                     │
          Write continues to kafka-3
```

---

## 11. Демонстраційний сценарій

```bash
#!/bin/bash
echo "=== Branch 16: Cluster & Replication Demo ==="

docker compose -f docker-compose-16.yml up --build -d
sleep 60

echo ""
echo "=== Стан кластера (3/3 броукерів) ==="
curl -s http://localhost:8081/api/cluster/brokers | jq '.[].brokerId'
curl -s http://localhost:8081/api/cluster/topic-info | jq .[0]

echo ""
echo "=== Відправка при 3 брокерах ==="
curl -s -X POST "http://localhost:8081/api/orders/batch?users=alice,bob&count=5" | jq .

echo ""
echo "=== Зупиняємо kafka-2 ==="
docker compose -f docker-compose-16.yml stop kafka-2
sleep 15
echo "ISR після зупинки:"
curl -s http://localhost:8081/api/cluster/topic-info | jq '.[].isr'
echo "Відправка при 2 брокерах:"
curl -s -X POST "http://localhost:8081/api/orders/batch?users=alice,bob&count=5" | jq .

echo ""
echo "=== Зупиняємо kafka-3 ==="
docker compose -f docker-compose-16.yml stop kafka-3
sleep 15
echo "ISR після 2 зупинок:"
curl -s http://localhost:8081/api/cluster/topic-info | jq '.[].isr'
echo "Відправка при 1 брокері (очікуємо ПОМИЛКУ):"
curl -s -X POST "http://localhost:8081/api/orders/batch?users=alice,bob&count=5" | jq .

echo ""
echo "=== Відновлення ==="
docker compose -f docker-compose-16.yml start kafka-2 kafka-3
sleep 30
echo "ISR після відновлення:"
curl -s http://localhost:8081/api/cluster/topic-info | jq '.[].isr'
curl -s -X POST "http://localhost:8081/api/orders/batch?users=alice,bob&count=5" | jq .
```

---

## 12. Питання для самоперевірки

- Що таке ISR і як брокер потрапляє або виходить з ISR?
- Яка різниця між `replication-factor` і `min.insync.replicas`?
- Що відбувається при падінні лідера партиції?
- Чому bootstrap-servers вказують кілька брокерів?
- При яких умовах виникає `NotEnoughReplicasException`?
- Що таке `unclean.leader.election.enable` і коли його варто увімкнути?
- Навіщо internal topics (`__consumer_offsets`) також мають RF=3?
- Скільки брокерів може впасти при RF=3, min.isr=2 щоб система продовжувала працювати?