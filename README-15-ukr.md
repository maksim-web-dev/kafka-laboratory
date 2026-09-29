# Branch 15 — Kafka Connect: CDC з Debezium та Elasticsearch

## 1. Що вивчаємо у цій гілці

- **Kafka Connect** — фреймворк для інтеграції Kafka з зовнішніми системами без написання
  consumer/producer коду.
- **CDC (Change Data Capture)** — захоплення кожної зміни в БД через WAL (Write-Ahead Log)
  замість polling.
- **Debezium** — CDC Source Connector для PostgreSQL: читає WAL, публікує у Kafka.
- **Elasticsearch Sink Connector** — зберігає CDC-події у пошуковому індексі.
- **SMT (Single Message Transform)** — `ExtractNewRecordState` розгортає Debezium-конверт,
  залишаючи лише `after`-стан.
- **`connect-init`** — сервіс-ініціалізатор, що автоматично реєструє конектори через REST API.

---

## 2. Зміни порівняно з попередньою гілкою (branch14)

- **Не використовується** Kafka producer у order-service — він просто пише в PostgreSQL.
- **Kafka Connect** замінює ручний producer для потрапляння даних у Kafka.
- **Додано** PostgreSQL (з WAL replication mode), Elasticsearch, Kibana.
- **Видалено** Avro / Schema Registry — JSON конвертер Kafka Connect.
- **order-service** тепер Spring Data JPA застосунок без Kafka залежностей.

---

## 3. Архітектура

```
order-service (Spring JPA)
      │  INSERT/UPDATE/DELETE
      ▼
PostgreSQL (WAL: wal_level=logical)
      │
      │  Debezium Source Connector
      │  (читає replication slot)
      ▼
Kafka topic: postgres.public.orders
      │
      │  Elasticsearch Sink Connector
      │  + SMT ExtractNewRecordState
      ▼
Elasticsearch index: postgres.public.orders
      │
      ▼
Kibana (пошук/візуалізація)
```

**Ключовий момент:** order-service **не знає** про Kafka. Він лише зберігає дані в PostgreSQL.
Debezium автоматично фіксує кожну зміну через WAL.

**Сервіси та порти (з docker-compose-15.yml):**
- `kafka` — 9092 (KRaft single-node)
- `kafka-ui` — 8080 → http://localhost:8080
- `postgres-b15` — 5432 → `jdbc:postgresql://localhost:5432/ordersdb`
- `kafka-connect-b15` — 8083 → http://localhost:8083
- `elasticsearch-b15` — 9200 → http://localhost:9200
- `kibana-b15` — 5601 → http://localhost:5601
- `order-service-b15` — 8081 → http://localhost:8081
- `connect-init-b15` — одноразовий контейнер (реєструє конектори)

---

## 4. Ключові концепції

### CDC через PostgreSQL WAL

```
WAL (Write-Ahead Log) — журнал всіх змін у PostgreSQL для crash recovery.
Debezium читає цей журнал через Logical Replication (pgoutput plugin).

Налаштування PostgreSQL для CDC:
  wal_level=logical          ← увімкнути logical replication
  max_replication_slots=4    ← кількість слотів для читання WAL
  max_wal_senders=4          ← кількість процесів що передають WAL

Debezium створює replication slot (зберігає позицію читання WAL):
  slot.name=debezium
```

### Kafka Connect REST API

```
Kafka Connect управляється через REST:

GET  /connectors                    → список конекторів
POST /connectors                    → реєстрація нового конектора
GET  /connectors/{name}/status      → стан конектора (RUNNING/FAILED/PAUSED)
DELETE /connectors/{name}           → видалення

Worker-процес Kafka Connect запускає конектори як tasks у своєму JVM.
```

### Debezium CDC-конверт

```json
{
  "before": null,
  "after": {
    "id": "550e8400...",
    "user_id": "alice",
    "total_amount": 149.99,
    "status": "PENDING",
    "created_at": "..."
  },
  "op": "c",
  "ts_ms": 1234567890123
}
```

- `op: "c"` = INSERT, `"u"` = UPDATE, `"d"` = DELETE, `"r"` = READ (snapshot)
- `before` = стан до операції (null для INSERT)
- `after` = стан після операції

### SMT ExtractNewRecordState

```json
{
  "transforms": "unwrap",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState"
}
```

Без SMT Elasticsearch отримує весь Debezium-конверт.
Після SMT — лише поля з `after`:
```json
{"id":"550e8400...","user_id":"alice","total_amount":149.99,"status":"PENDING"}
```

### Конфігурація конекторів

**Debezium Source (`debezium-source.json`):**
```json
{
  "name": "orders-source-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres-b15",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname": "ordersdb",
    "topic.prefix": "postgres",
    "table.include.list": "public.orders",
    "plugin.name": "pgoutput",
    "slot.name": "debezium"
  }
}
```

**Elasticsearch Sink (`elasticsearch-sink.json`):**
```json
{
  "name": "orders-sink-connector",
  "config": {
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "connection.url": "http://elasticsearch-b15:9200",
    "topics": "postgres.public.orders",
    "type.name": "_doc",
    "key.ignore": "true",
    "schema.ignore": "true",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState"
  }
}
```

---

## 5. Як запустити

```bash
# Перший запуск займає ~5 хвилин:
# kafka-connect встановлює плагіни через confluent-hub install
docker compose -f docker-compose-15.yml up --build

# connect-init автоматично реєструє обидва конектори
# Kafka UI: http://localhost:8080
# Kibana: http://localhost:5601
```

**Перевірка готовності:**
```bash
# Обидва конектори мають бути RUNNING
curl http://localhost:8083/connectors/orders-source-connector/status | jq .connector.state
curl http://localhost:8083/connectors/orders-sink-connector/status | jq .connector.state
# "RUNNING"
```

**Якщо connect-init не спрацював (ручна реєстрація):**
```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  --data @branch15_connect_cdc_debezium/connect/debezium-source.json

curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  --data @branch15_connect_cdc_debezium/connect/elasticsearch-sink.json
```

---

## 6. Як тестувати

### Крок 1 — Створити замовлення

```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId": "alice", "totalAmount": 149.99, "category": "ELECTRONICS"}'
```

### Крок 2 — Перевірити Kafka topic

```bash
# Kafka UI: http://localhost:8080 → Topics → postgres.public.orders
# Або через CLI:
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic postgres.public.orders \
  --from-beginning --max-messages 1 | jq .
# Побачимо Debezium-конверт з op="c"
```

### Крок 3 — Перевірити Elasticsearch

```bash
# Кількість документів
curl http://localhost:9200/postgres.public.orders/_count | jq .count

# Пошук all
curl "http://localhost:9200/postgres.public.orders/_search?pretty&size=3" | jq .hits.hits

# Пошук по userId
curl "http://localhost:9200/postgres.public.orders/_search?pretty" \
  -H "Content-Type: application/json" \
  -d '{"query":{"match":{"user_id":"alice"}}}' | jq .hits.total.value
```

### Крок 4 — Batch та UPDATE

```bash
# Пакет 10 замовлень
curl -X POST "http://localhost:8081/api/orders/batch?users=alice,bob,carol&count=10"

# Після UPDATE у PostgreSQL (якщо є API) — в Kafka буде op="u" з before та after
```

### Крок 5 — Kibana

```
Відкрити: http://localhost:5601
Management → Stack Management → Kibana → Data Views → Create data view
  Name: orders
  Index pattern: postgres.public.orders
  → Save
Discover → вибрати data view "orders" → побачити всі документи
```

---

## 7. Поглиблений розгляд

### Чому CDC краще за polling?

```
Polling (традиційний підхід):
  SELECT * FROM orders WHERE updated_at > last_check_time
  Проблема: потребує updated_at колонку, не фіксує DELETE,
            навантажує БД, можливі пропуски при race condition

CDC через WAL:
  Читає фізичний журнал БД — ВСІ операції (INSERT/UPDATE/DELETE)
  Незалежний від схеми таблиці
  Zero overhead на БД (WAL пишеться в будь-якому разі)
  Гарантований порядок подій
```

### Replication Slot

```
Replication Slot — PostgreSQL механізм що:
  1. Зберігає позицію читання WAL (LSN — Log Sequence Number)
  2. Гарантує що WAL не буде видалено поки slot не прочитає
  3. Debezium використовує slot для checkpoint

Небезпека: якщо Debezium зупинений надовго — WAL накопичується
(disk overflow!). В production треба моніторити pg_replication_slots.
```

### Kafka Connect Worker vs Task

```
Kafka Connect Worker (JVM процес):
  Виконує Tasks всередині себе
  Може виконувати кілька конекторів одночасно

Task — фактична unit of work:
  Debezium Source: 1 task (читає 1 replication slot)
  Elasticsearch Sink: N tasks (по одному на партицію/групу)

Distributed mode (кілька Workers):
  Tasks розподіляються між Workers автоматично
  → горизонтальне масштабування Connect
```

---

## 8. Структура проекту

```
branch15_connect_cdc_debezium/
├── connect/
│   ├── Dockerfile                    ← базовий CP Kafka Connect + confluent-hub install
│   ├── debezium-source.json          ← Debezium PostgreSQL connector config
│   └── elasticsearch-sink.json       ← Elasticsearch sink connector config
└── order-service/
    ├── controller/OrderController.kt ← REST API (без Kafka)
    ├── service/OrderService.kt        ← збереження в PostgreSQL
    ├── repository/OrderRepository.kt  ← Spring Data JPA
    └── model/Order.kt                 ← @Entity для ordersdb.public.orders
```

---

## 9. Що далі

- **Branch 16** — Multi-broker cluster: 3 Kafka brokers, Replication Factor, ISR, leader election,
  `NotEnoughReplicasException`.

---

## 10. Слайди для лекції

### Слайд 1 — Kafka Connect vs Custom Code

```
Custom producer/consumer:
  Write code → compile → test → deploy → maintain

Kafka Connect:
  Configure JSON → POST to REST API → RUNNING

Connect = reusable connectors for 200+ systems
(PostgreSQL, MySQL, MongoDB, S3, Elasticsearch, BigQuery, ...)
```

### Слайд 2 — CDC vs Polling

```
Polling (кожні 5 сек):           CDC (WAL-based):
  SELECT WHERE updated > last →    INSERT → WAL → Debezium → Kafka
  - потребує updated_at колонку    - ВСІ операції автоматично
  - не фіксує DELETE               - включно з DELETE
  - навантажує БД                  - zero overhead
  - може пропустити зміни          - гарантований порядок
```

### Слайд 3 — SMT ExtractNewRecordState

```
Без SMT:                          Після SMT:
{                                 {
  "before": null,      unwrap →     "id": "...",
  "after": {                        "user_id": "alice",
    "id": "...",                    "total_amount": 149.99
    "user_id": "alice"            }
  },
  "op": "c"
}
```

---

## 11. Демонстраційний сценарій

```bash
#!/bin/bash
echo "=== Branch 15: Kafka Connect CDC Demo ==="

docker compose -f docker-compose-15.yml up --build -d
echo "Очікуємо 5 хвилин (завантаження плагінів)..."
sleep 300

echo ""
echo "=== Перевірка конекторів ==="
curl -s http://localhost:8083/connectors/orders-source-connector/status | jq .connector.state
curl -s http://localhost:8083/connectors/orders-sink-connector/status | jq .connector.state

echo ""
echo "=== Крок 1: Створюємо замовлення ==="
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"alice","totalAmount":149.99,"category":"ELECTRONICS"}' | jq .

echo ""
echo "=== Крок 2: Kafka UI → postgres.public.orders ==="
echo "Відкрити: http://localhost:8080"
sleep 3

echo ""
echo "=== Крок 3: Перевірка Elasticsearch ==="
curl -s http://localhost:9200/postgres.public.orders/_count | jq .count

echo ""
echo "=== Крок 4: Batch — 10 замовлень ==="
curl -s -X POST "http://localhost:8081/api/orders/batch?users=alice,bob,carol&count=10"
sleep 3
curl -s http://localhost:9200/postgres.public.orders/_count | jq .count

echo ""
echo "=== Kibana: http://localhost:5601 ==="
echo "Створити Data View: postgres.public.orders"
```

---

## 12. Питання для самоперевірки

- Що таке WAL і чому він використовується для CDC?
- Навіщо PostgreSQL потрібен параметр `wal_level=logical`?
- Яка різниця між CDC і polling для фіксації змін у БД?
- Що таке replication slot і яка небезпека його накопичення?
- Що означає `op: "u"` у Debezium CDC-конверті?
- Навіщо потрібен SMT ExtractNewRecordState?
- Як Kafka Connect Worker відрізняється від Task?
- Як connect-init реєструє конектори? Чи можна це зробити вручну?