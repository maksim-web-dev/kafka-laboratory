---
marp: true
theme: default
paginate: true
backgroundColor: #ffffff
style: |
  section { font-family: 'Segoe UI', sans-serif; font-size: 28px; }
  section.title { text-align: center; justify-content: center; }
  h1 { color: #1a1a2e; }
  h2 { color: #16213e; border-bottom: 2px solid #0f3460; padding-bottom: 8px; }
  code { background: #f4f4f4; padding: 2px 6px; border-radius: 4px; }
  pre { background: #1e1e1e; color: #d4d4d4; border-radius: 8px; }
  blockquote { border-left: 4px solid #0f3460; padding-left: 16px; color: #555; }
---

<!-- _class: title -->

# Branch 22
# ksqlDB

**Apache Kafka for Certification & Production**

---

## Agenda

1. ksqlDB vs Kafka Streams — SQL vs Java
2. `STREAM` vs `TABLE` in ksqlDB
3. Push query — continuous real-time results
4. Pull query — point-in-time lookup
5. Windowed aggregations: TUMBLING, HOPPING, SESSION
6. `STREAM-TABLE JOIN` — enrich stream with table
7. ksqlDB REST API and CLI
8. Derived topics — where results are stored
9. Key Takeaways & What's Next

---

## ksqlDB vs Kafka Streams

**Same engine, different interface:**

```
Kafka Streams (Java/Kotlin):
  val ordersStream = builder.stream<String, OrderEvent>("orders")
  ordersStream
    .groupBy { _, v -> v.userId }
    .count()
    .toStream()
    .to("orders-count-by-user")
  → 15+ lines of code, compile, deploy JVM application

ksqlDB (SQL):
  CREATE TABLE orders_count_by_user AS
    SELECT userId, COUNT(*) AS orderCount
    FROM orders_stream
    GROUP BY userId
    EMIT CHANGES;
  → 5 lines of SQL, POST to REST API, instantly running
```

> Both compile to the same Kafka Streams topology under the hood.
> ksqlDB = SQL frontend for Kafka Streams.

---

## STREAM vs TABLE in ksqlDB

**Map SQL concepts to Kafka Streams primitives:**

```sql
-- STREAM: each row is an independent new event (KStream)
CREATE STREAM orders_stream (
    orderId  VARCHAR KEY,
    userId   VARCHAR,
    totalAmount DOUBLE,
    category VARCHAR
) WITH (
    KAFKA_TOPIC='22.orders.created',
    VALUE_FORMAT='JSON'
);

-- TABLE: each row is the latest state per key (KTable)
CREATE TABLE orders_count_by_user AS
    SELECT userId,
           COUNT(*) AS orderCount,
           SUM(totalAmount) AS totalSpent
    FROM orders_stream
    GROUP BY userId
    EMIT CHANGES;
```

> `STREAM` = every event is new. `TABLE` = latest value per key (upsert).

---

## Push Query — Continuous Real-Time Results

**`EMIT CHANGES`: query runs indefinitely, pushes new results:**

```sql
-- Push query: receive every new order as it arrives
SELECT * FROM orders_stream EMIT CHANGES;

-- Push query with filter
SELECT orderId, userId, totalAmount
FROM orders_stream
WHERE totalAmount >= 100
EMIT CHANGES;

-- Push query with windowed aggregation
SELECT category, COUNT(*) AS count
FROM orders_stream
WINDOW TUMBLING (SIZE 1 MINUTE)
GROUP BY category
EMIT CHANGES;
```

<br>

- Use for: live dashboards, real-time alerts, streaming ETL
- Client keeps HTTP connection open — new rows arrive as events
- Never returns — runs until client disconnects

---

## Pull Query — Point-in-Time Lookup

**No `EMIT CHANGES`: returns current state immediately:**

```sql
-- Pull query: get current order count for a user
SELECT * FROM orders_count_by_user
WHERE userId = 'user-42';
-- → returns immediately: {"userId":"user-42","orderCount":5,"totalSpent":249.95}

-- Pull query works ONLY on TABLE (not STREAM)
-- Equivalent to: HTTP GET /state?key=user-42
```

<br>

**Comparison:**

```
Push query:  SELECT ... FROM orders_stream EMIT CHANGES
             → infinite stream of new rows (WebSocket-like)
             → use for: dashboards, monitoring, data pipelines

Pull query:  SELECT ... FROM orders_count_by_user WHERE userId = 'X'
             → single response (REST-like)
             → use for: API endpoints, one-time checks
```

---

## Windowed Aggregations

**Three window types in SQL syntax:**

```sql
-- TUMBLING: fixed, non-overlapping windows
CREATE TABLE orders_per_category_1min AS
    SELECT category, COUNT(*) AS orderCount
    FROM orders_stream
    WINDOW TUMBLING (SIZE 1 MINUTES)
    GROUP BY category
    EMIT CHANGES;

-- HOPPING: overlapping windows (new window every 1 min, covers 5 min)
CREATE TABLE orders_per_user_hopping AS
    SELECT userId, COUNT(*) AS orderCount
    FROM orders_stream
    WINDOW HOPPING (SIZE 5 MINUTES, ADVANCE BY 1 MINUTE)
    GROUP BY userId
    EMIT CHANGES;

-- SESSION: dynamic windows based on activity gaps
-- Window closes after inactivity_gap of no events for a key
```

---

## Window Types — Visual Comparison

```
Input events: ●  ●  ●   ●    ●  ●  ●
time: ─────────────────────────────────►
              0  1  2   3    4  5  6

TUMBLING (size=2):
  [0─2)[2─4)[4─6)
  Event at t=1 appears in ONLY [0─2)

HOPPING (size=4, advance=2):
  [0─4)[2─6)[4─8)
  Event at t=3 appears in [0─4) AND [2─6) — in TWO windows

SESSION (inactivity_gap=2):
  [0──2]    [3]  [4──6]
       gap>2  gap>2
  Dynamic: window opens on first event, closes after silence
```

> **TUMBLING**: billing periods, independent time buckets
> **HOPPING**: moving averages, sliding metrics
> **SESSION**: user session analytics, click stream analysis

---

## STREAM-TABLE JOIN

**Enrich order stream with user data from a table:**

```sql
-- Create users table from a topic
CREATE TABLE users_table (
    userId VARCHAR PRIMARY KEY,
    name   VARCHAR,
    tier   VARCHAR
) WITH (KAFKA_TOPIC='22.users', VALUE_FORMAT='JSON');

-- Join stream with table to enrich orders
CREATE STREAM enriched_orders AS
    SELECT o.orderId,
           o.totalAmount,
           u.name    AS userName,
           u.tier    AS userTier
    FROM orders_stream o
    LEFT JOIN users_table u ON o.userId = u.userId
    EMIT CHANGES;
```

> Similar to KStream-KTable join but in SQL — co-partitioning still required.

---

## Key Takeaways

- **ksqlDB** = SQL frontend for Kafka Streams — same topology, no Java required
- **`STREAM`** = KStream (append-only); **`TABLE`** = KTable (latest per key)
- **Push query** (`EMIT CHANGES`) = continuous real-time results for dashboards
- **Pull query** (no `EMIT CHANGES`) = instant lookup, works only on TABLE
- **TUMBLING** = independent fixed windows; **HOPPING** = overlapping; **SESSION** = dynamic
- **ksqlDB REST API** = `POST /ksql` for DDL, `POST /query` for push/pull queries
- **Derived topics** = ksqlDB stores results as Kafka topics automatically

<br>

> ksqlDB democratizes stream processing: SQL skills are enough to build real-time pipelines.

---

## What's Next — Branch 23

**Security: SSL/TLS (SASL_SSL)**

- Full TLS encryption: traffic encrypted end-to-end
- `SASL_SSL` = SASL/PLAIN authentication + TLS encryption combined
- Certificate Authority (CA) → broker certificate → client truststore
- Keystore (my identity) vs Truststore (who I trust)
- `generate-certs.sh` — full certificate chain generation

---

<!-- _class: title -->

## What's Next — Branch 23
- SSL/TLS: encrypt all Kafka traffic
- CA → broker cert → client truststore chain
- SASL_SSL: auth + encryption combined
- generate-certs.sh: automated cert generation

# Questions?

**Branch 22 — ksqlDB**