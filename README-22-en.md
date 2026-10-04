# Branch 22 — ksqlDB

## 1. What we learn in this branch

- **ksqlDB** — SQL engine on top of Kafka Streams: stream processing via SQL instead of Java/Kotlin code.
- **STREAM vs TABLE** — STREAM = append-only log; TABLE = latest state per key.
- **Push query** — `EMIT CHANGES`: continuous query, returns new results in real time.
- **Pull query** — one-shot query (without EMIT CHANGES), like a SELECT in a relational DB.
- **Windowed aggregations** — TUMBLING, HOPPING, SESSION windows in SQL syntax.
- **STREAM-TABLE join** — enriching a stream with the current state of a table.

---

## 2. Changes compared to the previous branch (branch21)

- **Replaced** the product-catalog scenario with order analytics.
- **Added** ksqlDB Server and ksqlDB CLI containers.
- **order-service** publishes simplified orders (JSON, no Avro).
- **Removed** catalog-reader-service and compaction — focus is on streaming SQL.
- **New SQL objects**: 4 streams + 3 tables in ksqlDB.

---

## 3. Architecture

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

**SQL objects in ksqlDB:**
```sql
-- STREAMS:
orders_stream            ← over 22.orders.created
payments_stream          ← over 22.payments.processed
high_value_orders        ← filter: totalAmount >= 100
enriched_orders          ← stream-table join with orders_count_by_user

-- TABLES:
orders_count_by_user          ← COUNT(*) + SUM(totalAmount) per userId
orders_per_category_1min      ← COUNT(*) per category in tumbling 1-min window
orders_per_user_hopping       ← COUNT(*) per userId in hopping 5min/1min window
```

**Services and ports (from docker-compose-22.yml):**
- `kafka` — 9092 (KRaft single-node)
- `kafka-ui` — 8080 → http://localhost:8080
- `ksqldb-server-b22` — 8088 → http://localhost:8088
- `ksqldb-cli-b22` — interactive (no external port)
- `order-service-b22` — 8081 → http://localhost:8081

---

## 4. Key Concepts

### ksqlDB vs Kafka Streams

```
Kafka Streams (Java/Kotlin):
  val orders = builder.stream<String, OrderEvent>("orders")
  orders.groupByKey().count().toStream().to("orders-count")

ksqlDB (SQL):
  CREATE TABLE orders_count AS
    SELECT userId, COUNT(*) as cnt FROM orders_stream
    GROUP BY userId EMIT CHANGES;

Both compile to the same Kafka Streams engine under the hood.
ksqlDB = SQL frontend for Kafka Streams.
```

### STREAM vs TABLE in ksqlDB

```sql
-- STREAM: each record is a new event (append-only)
CREATE STREAM orders_stream (
  orderId VARCHAR KEY,
  userId VARCHAR,
  totalAmount DOUBLE,
  category VARCHAR
) WITH (KAFKA_TOPIC='22.orders.created', VALUE_FORMAT='JSON');

-- TABLE: each record is a state update per key (upsert)
CREATE TABLE orders_count_by_user AS
  SELECT userId, COUNT(*) AS orderCount, SUM(totalAmount) AS totalSpent
  FROM orders_stream
  GROUP BY userId
  EMIT CHANGES;
```

### Push query vs Pull query

```sql
-- PUSH query: continuous (for dashboards, real-time monitoring)
SELECT * FROM orders_stream EMIT CHANGES;
-- → infinite stream of new records

-- PUSH query with window:
SELECT category, COUNT(*) FROM orders_stream
  WINDOW TUMBLING (SIZE 1 MINUTE)
  GROUP BY category
  EMIT CHANGES;

-- PULL query: one-shot (for API requests)
SELECT * FROM orders_count_by_user WHERE userId = 'user-1';
-- → returns immediately, no EMIT CHANGES
-- Pull query works only with TABLE (not STREAM)
```

### Windowed aggregations

```sql
-- TUMBLING window: non-overlapping fixed-size windows
CREATE TABLE orders_per_category_1min AS
  SELECT category, COUNT(*) AS orderCount
  FROM orders_stream
  WINDOW TUMBLING (SIZE 1 MINUTES)
  GROUP BY category
  EMIT CHANGES;

-- HOPPING window: overlapping windows
CREATE TABLE orders_per_user_hopping AS
  SELECT userId, COUNT(*) AS orderCount
  FROM orders_stream
  WINDOW HOPPING (SIZE 5 MINUTES, ADVANCE BY 1 MINUTE)
  GROUP BY userId
  EMIT CHANGES;
-- A new 5-minute window appears every minute
```

---

## 5. How to Run

```bash
docker compose -f docker-compose-22.yml up --build

# Kafka UI: http://localhost:8080
# ksqlDB REST: http://localhost:8088
```

**Connect to CLI:**
```bash
docker exec -it ksqldb-cli-b22 ksql http://ksqldb-server-b22:8088
```

---

## 6. How to Test

### Step 1 — Initialize SQL objects

```bash
# Via CLI
docker exec -i ksqldb-cli-b22 ksql http://ksqldb-server-b22:8088 \
  < branch22_ksqldb/ksql/01_streams_and_tables.sql

# Verify they were created
docker exec -it ksqldb-cli-b22 ksql http://ksqldb-server-b22:8088 <<'EOF'
SHOW STREAMS;
SHOW TABLES;
EOF
```

### Step 2 — Send orders

```bash
# Single order
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-1","product":"Laptop","category":"ELECTRONICS","quantity":1,"totalAmount":1500.0}' | jq .

# Batch: 20 orders for 3 users
curl -s -X POST "http://localhost:8081/api/orders/batch?users=user-1,user-2,user-3&count=20" | jq .
```

### Step 3 — Push queries (continuous)

```bash
# CLI: new orders in real time
docker exec -it ksqldb-cli-b22 ksql http://ksqldb-server-b22:8088 <<'EOF'
SELECT * FROM orders_stream EMIT CHANGES LIMIT 5;
EOF

# Via REST API (streaming response):
curl -s http://localhost:8088/query-stream \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT userId, product, totalAmount FROM orders_stream EMIT CHANGES LIMIT 5;"}'
```

### Step 4 — Pull queries (point-in-time)

```bash
# CLI: how many orders for user-1?
docker exec -it ksqldb-cli-b22 ksql http://ksqldb-server-b22:8088 <<'EOF'
SELECT * FROM orders_count_by_user WHERE userId = 'user-1';
EOF

# Via REST:
curl -s -X POST http://localhost:8088/query \
  -H "Content-Type: application/json" \
  -d '{"ksql":"SELECT * FROM orders_count_by_user WHERE userId = '"'"'user-1'"'"';","streamsProperties":{"ksql.streams.auto.offset.reset":"earliest"}}' | jq .
```

### Step 5 — Windowed aggregation

```bash
docker exec -it ksqldb-cli-b22 ksql http://ksqldb-server-b22:8088 <<'EOF'
SELECT category, orderCount, WINDOWSTART, WINDOWEND
FROM orders_per_category_1min
EMIT CHANGES LIMIT 10;
EOF
```

### Step 6 — Check status

```bash
curl -s http://localhost:8088/info | jq .
curl -s http://localhost:8088/ksql \
  -H "Content-Type: application/json" \
  -d '{"ksql":"SHOW STREAMS;","streamsProperties":{}}' | jq .
```

---

## 7. Deep Dive

### ksqlDB as Kafka Streams under the hood

```
ksqlDB server compiles SQL → Kafka Streams topology
  CREATE TABLE orders_count_by_user AS SELECT...
  ↓ compiles to:
  KStream.groupBy(userId).count().toStream().to(internal_topic)

ksqlDB internal topics:
  _confluent-ksql-ksql_service_22_query-CSAS-...
  _confluent-ksql-ksql_service_22_query-CTAS-...
```

### Pull query limitation

```
Pull query (without EMIT CHANGES):
  ✓ Supported for TABLE
  ✗ NOT supported for STREAM

Reason: TABLE has a materialized state store → snapshot can be queried
        STREAM is an infinite event log → no "current state"

Workaround: if a pull query is needed on a stream → GROUP BY → TABLE
```

### Session Window vs Tumbling vs Hopping

```
TUMBLING (SIZE 1 MINUTE):
  [0:00─1:00) [1:00─2:00) [2:00─3:00)
  No overlap, fixed size

HOPPING (SIZE 5 MINUTES, ADVANCE BY 1 MINUTE):
  [0:00─5:00) [1:00─6:00) [2:00─7:00) ...
  With overlap, "sliding" window

SESSION (INACTIVITY_GAP 30 SECONDS):
  Window closes after 30 sec of silence
  Size depends on activity
  Useful for user session analytics
```

---

## 8. Project Structure

```
branch22_ksqldb/
├── ksql/
│   └── 01_streams_and_tables.sql    ← DDL for all streams and tables
├── order-service/
│   ├── controller/OrderController.kt ← POST /api/orders, POST /api/orders/batch
│   └── service/OrderService.kt       ← publish to 22.orders.created (JSON)
```

**File 01_streams_and_tables.sql:**
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

## 9. What's Next

- **Branch 23** — SSL/TLS: SASL_SSL = SASL + SSL traffic encryption, certificate generation,
  JKS keystores.

---

## 10. Lecture Slides

### Slide 1 — ksqlDB vs Custom Code

```
Kafka Streams (Java):              ksqlDB (SQL):
  ordersStream                       CREATE TABLE order_count AS
    .groupBy((k, v) -> v.userId)       SELECT userId, COUNT(*)
    .count()                           FROM orders_stream
    .toStream()                        GROUP BY userId
    .to("counts")                      EMIT CHANGES;

  15 lines of code                   4 lines of SQL
```

### Slide 2 — Push vs Pull query

```
Push query (real-time stream):      Pull query (point-in-time):
  SELECT * FROM orders_stream         SELECT *
  EMIT CHANGES;                       FROM orders_count_by_user
  ──────────────────────────►         WHERE userId = 'user-1';
  [order1][order2][order3]...         [{"userId":"user-1","count":5}]
  (infinite stream)                   (returns immediately)

  Suitable for: dashboards, alerts    Suitable for: API endpoints
```

### Slide 3 — Window Types

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

## 11. Demo Script

```bash
#!/bin/bash
echo "=== Branch 22: ksqlDB Demo ==="

docker compose -f docker-compose-22.yml up --build -d
sleep 90

echo ""
echo "=== Initialize SQL objects ==="
docker exec -i ksqldb-cli-b22 ksql http://ksqldb-server-b22:8088 \
  < branch22_ksqldb/ksql/01_streams_and_tables.sql

echo ""
echo "=== Generate orders ==="
curl -s -X POST "http://localhost:8081/api/orders/batch?users=alice,bob,charlie&count=20"
sleep 5

echo ""
echo "=== Pull query: how many orders per user? ==="
docker exec -it ksqldb-cli-b22 ksql http://ksqldb-server-b22:8088 <<'EOF'
SELECT * FROM orders_count_by_user;
EOF

echo ""
echo "=== Push query via REST: high-value orders ==="
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

## 12. Self-Check Questions

- What is the difference between a ksqlDB STREAM and a TABLE?
- Why is a pull query not supported for a STREAM?
- What is the difference between a push and pull query in ksqlDB?
- What happens under the hood when `CREATE TABLE AS SELECT...` is executed?
- What is the difference between a TUMBLING and a HOPPING window?
- When should you use a SESSION window?
- How does ksqlDB differ from the Kafka Streams API?
- How does ksqlDB store state for a TABLE with GROUP BY?