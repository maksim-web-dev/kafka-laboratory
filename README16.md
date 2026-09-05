# Branch 16 — Cluster & Replication

## Що вивчаємо

- **Multi-broker cluster** — 3 брокери Kafka в одному Docker Compose (KRaft mode)
- **Replication Factor** — скільки копій партиції зберігається в кластері
- **ISR (In-Sync Replicas)** — підмножина реплік, що синхронізовані з лідером
- **Leader Election** — автоматичне перепризначення лідера при падінні брокера
- **`min.insync.replicas`** — мінімум ISR для прийняття запису (`acks=all`)
- **`NotEnoughReplicasException`** — що відбувається, коли ISR < min.insync.replicas

## Архітектура

```
                 ┌─────────────────────────────────┐
                 │  Kafka Cluster (KRaft, 3 nodes)  │
                 │                                  │
  order-service ─►  kafka-1 (broker+controller)     │
   acks=all      │  kafka-2 (broker+controller)     │
                 │  kafka-3 (broker+controller)     │
                 └───────────┬─────────────────────┘
                             │
                    topic: 16.orders.created
                    partitions=3, replicas=3
                    min.insync.replicas=2
                             │
                   notification-service
```

**Топік `16.orders.created`:**
- 3 партиції → розподілені по 3 брокерам
- replication-factor=3 → кожна партиція має 3 копії
- min.insync.replicas=2 → для успішного запису потрібно 2+ ISR

## Сервіси та порти

| Сервіс | Порт |
|--------|------|
| kafka-1 (external) | 9117 |
| kafka-2 (external) | 9118 |
| kafka-3 (external) | 9119 |
| Kafka UI | 8168 → http://localhost:8168 |
| order-service | 8169 → http://localhost:8169 |
| notification-service | 8170 |

## Запуск

```bash
docker compose -f docker-compose-16.yml up --build
```

## REST API

### Перевірити стан кластера

```bash
# Активні брокери та поточний controller
curl http://localhost:8169/api/cluster/brokers

# Лідер, ISR та стан реплікації для кожної партиції
curl http://localhost:8169/api/cluster/topic-info
```

### Відправити замовлення

```bash
curl -X POST http://localhost:8169/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId": "alice", "totalAmount": 99.99, "category": "FOOD"}'

# Пакет замовлень
curl -X POST "http://localhost:8169/api/orders/batch?users=alice,bob,carol&count=20"
```

---

## Демо 1 — Нормальна робота (3/3 брокерів)

```bash
# ISR повний: [1, 2, 3] для кожної партиції
curl http://localhost:8169/api/cluster/topic-info

# Надіслати замовлення — всі успішно
curl -X POST "http://localhost:8169/api/orders/batch?users=alice,bob&count=5"
```

**Що спостерігати в Kafka UI (http://localhost:8168):**
- Topics → `16.orders.created` → Partitions: лідер та ISR для кожної

---

## Демо 2 — Падіння одного брокера (2/3 ISR)

```bash
# Зупинити kafka-2
docker compose -f docker-compose-16.yml stop kafka-2

# ISR стає [1, 3] — under-replicated, але 2 >= min.insync.replicas=2
curl http://localhost:8169/api/cluster/topic-info

# Замовлення ПРОДОВЖУЮТЬ надходити (2 ISR достатньо)
curl -X POST "http://localhost:8169/api/orders/batch?users=alice,bob&count=5"
```

**Що відбувається:**
- Kafka перепризначає лідера для партицій, де kafka-2 був лідером
- ISR зменшується з [1,2,3] до [1,3]
- Виробництво продовжується (`underReplicated: true`, але не заблоковано)

---

## Демо 3 — Падіння двох брокерів (1/3 ISR < min.insync.replicas)

```bash
# Зупинити kafka-3 (kafka-2 вже зупинений)
docker compose -f docker-compose-16.yml stop kafka-3

# ISR стає [1] — лише 1 < min.insync.replicas=2
curl http://localhost:8169/api/cluster/topic-info

# ПОМИЛКА! NotEnoughReplicasException
curl -X POST "http://localhost:8169/api/orders/batch?users=alice,bob&count=5"
```

**Очікуваний результат:**
```json
{"requested": 5, "sent": 0, "failed": 5}
```

Лог order-service:
```
[REPL] Send failed: org.apache.kafka.common.errors.NotEnoughReplicasException:
  Messages are rejected since there are fewer in-sync replicas than required.
```

---

## Демо 4 — Відновлення брокера

```bash
# Запустити kafka-2 і kafka-3 назад
docker compose -f docker-compose-16.yml start kafka-2 kafka-3

# ISR відновлюється до [1, 2, 3]
curl http://localhost:8169/api/cluster/topic-info

# Виробництво відновлюється
curl -X POST "http://localhost:8169/api/orders/batch?users=alice,bob&count=5"
```

---

## Ключові концепції

### ISR (In-Sync Replicas)

```
Partition 0:
  Leader:   broker-1
  Replicas: [1, 2, 3]
  ISR:      [1, 2, 3]  ← всі синхронізовані

Після зупинки broker-2:
  Leader:   broker-1
  Replicas: [1, 2, 3]
  ISR:      [1, 3]     ← broker-2 виключено з ISR
```

### acks=all + min.insync.replicas

```
acks=all → producer чекає підтвердження від ВСІХ ISR
min.insync.replicas=2 → потрібно мінімум 2 ISR

Якщо ISR=3: запис успішний ✓
Якщо ISR=2: запис успішний ✓ (2 >= 2)
Якщо ISR=1: NotEnoughReplicasException ✗ (1 < 2)
```

### Replication Factor рекомендації

| Середовище | Replication Factor | min.insync.replicas |
|------------|-------------------|---------------------|
| Розробка | 1 | 1 |
| Staging | 2 | 1 |
| Production | 3 | 2 |

## Зупинка

```bash
docker compose -f docker-compose-16.yml down
```