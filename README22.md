# Branch 22 — ksqlDB

## Що вивчаємо

| Концепція | Деталі |
|-----------|--------|
| **STREAM vs TABLE** | STREAM = append-only лог подій; TABLE = останній стан per key |
| **Push query** | Continuous query з `EMIT CHANGES` — повертає нові результати в реальному часі |
| **Pull query** | Одноразовий запит (без `EMIT CHANGES`) — як SELECT у звичайній БД |
| **Windowed aggregations** | TUMBLING, HOPPING, SESSION вікна у SQL синтаксисі |
| **Joins у ksqlDB** | STREAM-TABLE join, STREAM-STREAM join у SQL |

## Що таке ksqlDB

ksqlDB — це SQL-рушій поверх Kafka Streams. Замість написання Java/Kotlin коду ви описуєте потокову обробку SQL-запитами.

```
Без ksqlDB (Kafka Streams):
  val ordersStream = builder.stream("orders")
  ordersStream.groupBy(userId).count().toStream().to("orders-by-user")

З ksqlDB:
  CREATE TABLE orders_count AS
    SELECT userId, COUNT(*) FROM orders_stream GROUP BY userId EMIT CHANGES;
```

## Архітектура

```
order-service ──[22.orders.created]──► ksqlDB server
              ──[22.payments.processed]──►  │
                                            │  STREAMS + TABLES
                                            │  SQL queries
                                            ▼
                                        derived topics:
                                        KSQL_ORDERS_COUNT_BY_USER
                                        KSQL_HIGH_VALUE_ORDERS
                                        KSQL_ORDERS_PER_CATEGORY_1MIN
```

## Порти

| Сервіс | Порт |
|--------|------|
| Kafka (external) | 9127 |
| Kafka UI | 8204 |
| ksqlDB Server | 8088 |
| ksqlDB CLI | (інтерактивний, без порту) |
| order-service | 8205 |

## Запуск

```bash
docker compose -f docker-compose-22.yml up --build
```

---

## Підключення до ksqlDB CLI

```bash
docker exec -it ksqldb-cli-b22 ksql http://ksqldb-server-b22:8088
```

Або через REST API:
```bash
# Перевірити статус
curl -s http://localhost:8088/info | jq .

# Виконати запит
curl -s http://localhost:8088/ksql \
  -H "Content-Type: application/json" \
  -d '{"ksql": "SHOW STREAMS;", "streamsProperties": {}}' | jq .
```

---

## Демо

### Крок 1 — Ініціалізувати SQL об'єкти

```bash
# Виконати SQL scripts у ksqlDB CLI
docker exec -i ksqldb-cli-b22 ksql http://ksqldb-server-b22:8088 \
  < branch22_ksqldb/ksql/01_streams_and_tables.sql

# Перевірити що створилось
docker exec -it ksqldb-cli-b22 ksql http://ksqldb-server-b22:8088 <<'EOF'
SHOW STREAMS;
SHOW TABLES;
EOF
```

### Крок 2 — Надіслати замовлення

```bash
# Одне замовлення
curl -s -X POST http://localhost:8205/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-1","product":"Laptop","category":"ELECTRONICS","quantity":1,"totalAmount":1500.0}' | jq

# Batch: 20 замовлень для 3 користувачів
curl -s -X POST "http://localhost:8205/api/orders/batch?users=user-1,user-2,user-3&count=20" | jq
```

### Крок 3 — Push queries (continuous)

```bash
# У ksqlDB CLI — бачимо нові замовлення в реальному часі
docker exec -it ksqldb-cli-b22 ksql http://ksqldb-server-b22:8088 <<'EOF'
SELECT * FROM orders_stream EMIT CHANGES LIMIT 5;
EOF

# Або через REST (streaming response):
curl -s http://localhost:8088/query-stream \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT userId, product, totalAmount FROM orders_stream EMIT CHANGES LIMIT 5;"}' 
```

### Крок 4 — Pull queries (point-in-time)

```bash
# Скільки замовлень зробив user-1? (pull query — одноразовий)
docker exec -it ksqldb-cli-b22 ksql http://ksqldb-server-b22:8088 <<'EOF'
SELECT * FROM orders_count_by_user WHERE userId = 'user-1';
EOF

# Через REST:
curl -s -X POST http://localhost:8088/query \
  -H "Content-Type: application/json" \
  -d '{"ksql": "SELECT * FROM orders_count_by_user WHERE userId = '\''user-1'\'';", "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"}}' | jq
```

### Крок 5 — Windowed aggregation

```bash
# Кількість замовлень per категорія за останню хвилину (tumbling window)
docker exec -it ksqldb-cli-b22 ksql http://ksqldb-server-b22:8088 <<'EOF'
SELECT category, orderCount, WINDOWSTART, WINDOWEND
FROM orders_per_category_1min
EMIT CHANGES LIMIT 10;
EOF
```

### Крок 6 — Stream-Table join

```bash
# Збагачений потік замовлень з агрегованими даними по user
docker exec -it ksqldb-cli-b22 ksql http://ksqldb-server-b22:8088 <<'EOF'
SELECT orderId, userId, totalAmount, userTotalOrders, userTotalSpent
FROM enriched_orders EMIT CHANGES LIMIT 5;
EOF
```

---

## SQL об'єкти (overview)

```sql
-- STREAMS (джерела та похідні):
orders_stream          ← базовий stream над 22.orders.created
payments_stream        ← базовий stream над 22.payments.processed
high_value_orders      ← filter: totalAmount >= 100
enriched_orders        ← stream-table join з orders_count_by_user

-- TABLES (агрегації):
orders_count_by_user        ← COUNT(*) + SUM(totalAmount) per userId
orders_per_category_1min    ← COUNT(*) per category у tumbling 1-min window
orders_per_user_hopping     ← COUNT(*) per userId у hopping 5min/1min window
```

---

## STREAM vs TABLE — різниця

| | STREAM | TABLE |
|--|--|--|
| **Семантика** | Кожен запис — нова подія | Кожен запис — оновлення стану per key |
| **Аналогія** | Журнал транзакцій | Поточний стан таблиці |
| **EMIT CHANGES** | Кожне нове повідомлення | Кожна зміна стану |
| **Pull query** | НЕ підтримується | Підтримується |
| **Приклад** | orders_stream | orders_count_by_user |

## Push vs Pull query

```sql
-- PUSH query: continuous streaming (підходить для dashboards, alerts)
SELECT * FROM orders_stream EMIT CHANGES;   ← нескінченний потік нових записів

-- PULL query: одноразовий snapshot (підходить для API запитів)
SELECT * FROM orders_count_by_user WHERE userId = 'user-1';  ← повертає одразу
```

---

## Ключові концепції

| Концепція | Що демонструє |
|-----------|---------------|
| **CREATE STREAM** | Оголошення потоку над Kafka topic |
| **CREATE TABLE AS SELECT** | Materialized aggregation (backed by state store) |
| **EMIT CHANGES** | Push query — continuous результати |
| **Pull query** | Point-in-time SELECT з TABLE |
| **TUMBLING window** | Не-перекривні вікна фіксованого розміру |
| **HOPPING window** | Перекривні вікна (SIZE > ADVANCE BY) |
| **STREAM-TABLE join** | Збагачення stream поточним станом TABLE |
| **ksqlDB vs Kafka Streams** | SQL vs Java/Kotlin code — той самий движок (Kafka Streams) |