# Branch 22 — ksqlDB

## 1. Що вивчаємо у цій гілці

- **ksqlDB** — SQL-рушій поверх Kafka Streams: потокова обробка через SQL замість Java/Kotlin коду.
- **STREAM vs TABLE** — STREAM = append-only лог; TABLE = останній стан per key.
- **Push query** — `EMIT CHANGES`: continuous query, повертає нові результати в реальному часі.
- **Pull query** — одноразовий запит (без EMIT CHANGES), як SELECT у реляційній БД.
- **Windowed aggregations** — TUMBLING, HOPPING, SESSION вікна у SQL синтаксисі.
- **STREAM-TABLE join** — збагачення потоку поточним станом таблиці.

---

## 2. Зміни порівняно з попередньою гілкою (branch21)

- **Замінено** product-catalog сценарій на order analytics.
- **Додано** ksqlDB Server та ksqlDB CLI контейнери.
- **order-service** публікує спрощені замовлення (JSON, без Avro).
- **Видалено** catalog-reader-service та compaction — фокус на streaming SQL.
- **Нові SQL об'єкти**: 4 streams + 3 tables у ksqlDB.

---

## 3. Архітектура

```
order-service ──[22.orders.created]──► ksqlDB Server
              ──[22.payments.processed]──►    │
                                              │  SQL processing
                                              │
                              derived topics (ksqlDB internal):
                               KSQL_ORDERS_COUNT_BY_USER
                               KSQL_HIGH_VALUE_ORDERS
                               KSQL_ORDERS_PER_CATEGORY_1MIN
```

**SQL об'єкти у ksqlDB:**
```sql
-- STREAMS:
orders_stream            ← над 22.orders.created
payments_stream          ← над 22.payments.processed
high_value_orders        ← filter: totalAmount >= 100
enriched_orders          ← stream-table join з orders_count_by_user

-- TABLES:
orders_count_by_user          ← COUNT(*) + SUM(totalAmount) per userId
orders_per_category_1min      ← COUNT(*) per category у tumbling 1-min window
orders_per_user_hopping       ← COUNT(*) per userId у hopping 5min/1min window
```

**Сервіси та порти (з docker-compose-22.yml):**
- `kafka` — 9092 (KRaft single-node)
- `kafka-ui` — 8080 → http://localhost:8080
- `ksqldb-server-b22` — 8088 → http://localhost:8088
- `ksqldb-cli-b22` — інтерактивний (без external port)
- `order-service-b22` — 8081 → http://localhost:8081

---

## 4. Ключові концепції

### ksqlDB vs Kafka Streams

```
Kafka Streams (Java/Kotlin):
  val orders = builder.stream<String, OrderEvent>("orders")
  orders.groupByKey().count().toStream().to("orders-count")

ksqlDB (SQL):
  CREATE TABLE orders_count AS
    SELECT userId, COUNT(*) as cnt FROM orders_stream
    GROUP BY userId EMIT CHANGES;

Обидва компілюються в один Kafka Streams рушій під капотом.
ksqlDB = SQL-frontend для Kafka Streams.
```

### STREAM vs TABLE у ksqlDB

```sql
-- STREAM: кожен запис — нова подія (append-only)
CREATE STREAM orders_stream (
  orderId VARCHAR KEY,
  userId VARCHAR,
  totalAmount DOUBLE,
  category VARCHAR
) WITH (KAFKA_TOPIC='22.orders.created', VALUE_FORMAT='JSON');

-- TABLE: кожен запис — оновлення стану per key (upsert)
CREATE TABLE orders_count_by_user AS
  SELECT userId, COUNT(*) AS orderCount, SUM(totalAmount) AS totalSpent
  FROM orders_stream
  GROUP BY userId
  EMIT CHANGES;
```

### Push query vs Pull query

```sql
-- PUSH query: continuous (для dashboards, real-time monitoring)
SELECT * FROM orders_stream EMIT CHANGES;
-- → нескінченний потік нових записів

-- PUSH query з вікном:
SELECT category, COUNT(*) FROM orders_stream
  WINDOW TUMBLING (SIZE 1 MINUTE)
  GROUP BY category
  EMIT CHANGES;

-- PULL query: one-shot (для API запитів)
SELECT * FROM orders_count_by_user WHERE userId = 'user-1';
-- → повертає одразу, без EMIT CHANGES
-- Pull query працює тільки з TABLE (не зі STREAM)
```

### Windowed aggregations

```sql
-- TUMBLING window: невідновлювані вікна фіксованого розміру
CREATE TABLE orders_per_category_1min AS
  SELECT category, COUNT(*) AS orderCount
  FROM orders_stream
  WINDOW TUMBLING (SIZE 1 MINUTES)
  GROUP BY category
  EMIT CHANGES;

-- HOPPING window: вікна що перекриваються
CREATE TABLE orders_per_user_hopping AS
  SELECT userId, COUNT(*) AS orderCount
  FROM orders_stream
  WINDOW HOPPING (SIZE 5 MINUTES, ADVANCE BY 1 MINUTE)
  GROUP BY userId
  EMIT CHANGES;
-- Кожну хвилину з'являється нове 5-хвилинне вікно
```

---

## 5. Як запустити

```bash
docker compose -f docker-compose-22.yml up --build

# Kafka UI: http://localhost:8080
# ksqlDB REST: http://localhost:8088
```

**Підключення до CLI:**
```bash
docker exec -it ksqldb-cli-b22 ksql http://ksqldb-server-b22:8088
```

---

## 6. Як тестувати

### Крок 1 — Ініціалізувати SQL об'єкти

```bash
# Через CLI
docker exec -i ksqldb-cli-b22 ksql http://ksqldb-server-b22:8088 \
  < branch22_ksqldb/ksql/01_streams_and_tables.sql

# Переконатись що створилось
docker exec -it ksqldb-cli-b22 ksql http://ksqldb-server-b22:8088 <<'EOF'
SHOW STREAMS;
SHOW TABLES;
EOF
```

### Крок 2 — Надіслати замовлення

```bash
# Одне замовлення
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-1","product":"Laptop","category":"ELECTRONICS","quantity":1,"totalAmount":1500.0}' | jq .

# Batch: 20 замовлень для 3 користувачів
curl -s -X POST "http://localhost:8081/api/orders/batch?users=user-1,user-2,user-3&count=20" | jq .
```

### Крок 3 — Push queries (continuous)

```bash
# CLI: нові замовлення в реальному часі
docker exec -it ksqldb-cli-b22 ksql http://ksqldb-server-b22:8088 <<'EOF'
SELECT * FROM orders_stream EMIT CHANGES LIMIT 5;
EOF

# Через REST API (streaming response):
curl -s http://localhost:8088/query-stream \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT userId, product, totalAmount FROM orders_stream EMIT CHANGES LIMIT 5;"}'
```

### Крок 4 — Pull queries (point-in-time)

```bash
# CLI: скільки замовлень user-1?
docker exec -it ksqldb-cli-b22 ksql http://ksqldb-server-b22:8088 <<'EOF'
SELECT * FROM orders_count_by_user WHERE userId = 'user-1';
EOF

# Через REST:
curl -s -X POST http://localhost:8088/query \
  -H "Content-Type: application/json" \
  -d '{"ksql":"SELECT * FROM orders_count_by_user WHERE userId = '"'"'user-1'"'"';","streamsProperties":{"ksql.streams.auto.offset.reset":"earliest"}}' | jq .
```

### Крок 5 — Windowed aggregation

```bash
docker exec -it ksqldb-cli-b22 ksql http://ksqldb-server-b22:8088 <<'EOF'
SELECT category, orderCount, WINDOWSTART, WINDOWEND
FROM orders_per_category_1min
EMIT CHANGES LIMIT 10;
EOF
```

### Крок 6 — Перевірити статус

```bash
curl -s http://localhost:8088/info | jq .
curl -s http://localhost:8088/ksql \
  -H "Content-Type: application/json" \
  -d '{"ksql":"SHOW STREAMS;","streamsProperties":{}}' | jq .
```

---

## 7. Поглиблений розгляд

### ksqlDB як Kafka Streams під капотом

```
ksqlDB server компілює SQL → Kafka Streams topology
  CREATE TABLE orders_count_by_user AS SELECT...
  ↓ компілюється у:
  KStream.groupBy(userId).count().toStream().to(internal_topic)

Внутрішні топіки ksqlDB:
  _confluent-ksql-ksql_service_22_query-CSAS-...
  _confluent-ksql-ksql_service_22_query-CTAS-...
```

### Pull query limitation

```
Pull query (без EMIT CHANGES):
  ✓ Підтримується для TABLE
  ✗ НЕ підтримується для STREAM

Причина: TABLE має materialized state store → можна запитати snapshot
         STREAM — це нескінченний потік подій → немає "поточного стану"

Компроміс: якщо потрібен pull query зі stream → GROUP BY → TABLE
```

### Session Window vs Tumbling vs Hopping

```
TUMBLING (SIZE 1 MINUTE):
  [0:00─1:00) [1:00─2:00) [2:00─3:00)
  Без overlap, фіксований розмір

HOPPING (SIZE 5 MINUTES, ADVANCE BY 1 MINUTE):
  [0:00─5:00) [1:00─6:00) [2:00─7:00) ...
  З overlap, "sliding" window

SESSION (INACTIVITY_GAP 30 SECONDS):
  Вікно закривається після 30 сек тиші
  Розмір залежить від активності
  Корисно для user session analytics
```

---

## 8. Структура проекту

```
branch22_ksqldb/
├── ksql/
│   └── 01_streams_and_tables.sql    ← DDL для всіх streams та tables
├── order-service/
│   ├── controller/OrderController.kt ← POST /api/orders, POST /api/orders/batch
│   └── service/OrderService.kt       ← publish to 22.orders.created (JSON)
```

**Файл 01_streams_and_tables.sql:**
```sql
CREATE STREAM IF NOT EXISTS orders_stream (
  orderId VARCHAR KEY,
  userId VARCHAR,
  product VARCHAR,
  category VARCHAR,
  quantity INT,
  totalAmount DOUBLE
) WITH (KAFKA_TOPIC='22.orders.created', VALUE_FORMAT='JSON', PARTITIONS=3);

CREATE TABLE IF NOT EXISTS orders_count_by_user AS
  SELECT userId, COUNT(*) AS orderCount, SUM(totalAmount) AS totalSpent
  FROM orders_stream GROUP BY userId EMIT CHANGES;

CREATE STREAM IF NOT EXISTS high_value_orders AS
  SELECT * FROM orders_stream WHERE totalAmount >= 100 EMIT CHANGES;

CREATE TABLE IF NOT EXISTS orders_per_category_1min AS
  SELECT category, COUNT(*) AS orderCount
  FROM orders_stream WINDOW TUMBLING (SIZE 1 MINUTES)
  GROUP BY category EMIT CHANGES;
```

---

## 9. Що далі

- **Branch 23** — SSL/TLS: SASL_SSL = SASL + SSL шифрування трафіку, генерація сертифікатів,
  JKS keystores.

---

## 10. Слайди для лекції

### Слайд 1 — ksqlDB vs Custom Code

```
Kafka Streams (Java):              ksqlDB (SQL):
  ordersStream                       CREATE TABLE order_count AS
    .groupBy((k, v) -> v.userId)       SELECT userId, COUNT(*)
    .count()                           FROM orders_stream
    .toStream()                        GROUP BY userId
    .to("counts")                      EMIT CHANGES;

  15 рядків коду                     4 рядки SQL
```

### Слайд 2 — Push vs Pull query

```
Push query (real-time stream):      Pull query (point-in-time):
  SELECT * FROM orders_stream         SELECT *
  EMIT CHANGES;                       FROM orders_count_by_user
  ──────────────────────────►         WHERE userId = 'user-1';
  [order1][order2][order3]...         [{"userId":"user-1","count":5}]
  (нескінченний потік)                (повертає одразу)

  Підходить: dashboards, alerts       Підходить: API endpoints
```

### Слайд 3 — Window Types

```
Input:    ●  ●  ●   ●    ●  ●  ●
time: ────────────────────────────►
          0  1  2   3    4  5  6

TUMBLING:  [0─2][2─4][4─6]
HOPPING:   [0─3][1─4][2─5][3─6][4─7]
SESSION:   [0──2]    [3]  [4──6]
                  gap > inactivity_gap
```

---

## 11. Демонстраційний сценарій

```bash
#!/bin/bash
echo "=== Branch 22: ksqlDB Demo ==="

docker compose -f docker-compose-22.yml up --build -d
sleep 90

echo ""
echo "=== Ініціалізація SQL об'єктів ==="
docker exec -i ksqldb-cli-b22 ksql http://ksqldb-server-b22:8088 \
  < branch22_ksqldb/ksql/01_streams_and_tables.sql

echo ""
echo "=== Генерація замовлень ==="
curl -s -X POST "http://localhost:8081/api/orders/batch?users=alice,bob,charlie&count=20"
sleep 5

echo ""
echo "=== Pull query: скільки замовлень per user? ==="
docker exec -it ksqldb-cli-b22 ksql http://ksqldb-server-b22:8088 <<'EOF'
SELECT * FROM orders_count_by_user;
EOF

echo ""
echo "=== Push query через REST: high-value orders ==="
curl -s http://localhost:8088/query-stream \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT * FROM high_value_orders EMIT CHANGES LIMIT 3;"}'

echo ""
echo "=== Windowed aggregation ==="
docker exec -it ksqldb-cli-b22 ksql http://ksqldb-server-b22:8088 <<'EOF'
SELECT category, orderCount FROM orders_per_category_1min EMIT CHANGES LIMIT 5;
EOF
```

---

## 12. Питання для самоперевірки

- Яка різниця між ksqlDB STREAM та TABLE?
- Чому pull query не підтримується для STREAM?
- Яка різниця між push та pull query у ksqlDB?
- Що відбувається під капотом при `CREATE TABLE AS SELECT...`?
- Яка різниця між TUMBLING та HOPPING window?
- Коли використовувати SESSION window?
- Чим ksqlDB відрізняється від Kafka Streams API?
- Як ksqlDB зберігає state для TABLE з GROUP BY?