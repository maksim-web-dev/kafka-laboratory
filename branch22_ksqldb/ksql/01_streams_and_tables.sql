-- ============================================================
-- Branch 22 — ksqlDB: Streams, Tables, Joins, Windows
-- Run in ksqlDB CLI:
--   docker exec -i ksqldb-cli-b22 ksql http://ksqldb-server-b22:8088 < 01_streams_and_tables.sql
-- ============================================================

-- === BASE STREAMS (over existing Kafka topics) ===

-- Оголошуємо stream над topic 22.orders.created
CREATE STREAM IF NOT EXISTS orders_stream (
  orderId  VARCHAR KEY,
  userId   VARCHAR,
  product  VARCHAR,
  category VARCHAR,
  quantity INT,
  totalAmount DOUBLE,
  timestamp VARCHAR
) WITH (
  KAFKA_TOPIC   = '22.orders.created',
  VALUE_FORMAT  = 'JSON',
  PARTITIONS    = 3
);

-- Оголошуємо stream над topic 22.payments.processed
CREATE STREAM IF NOT EXISTS payments_stream (
  paymentId VARCHAR KEY,
  orderId   VARCHAR,
  userId    VARCHAR,
  amount    DOUBLE,
  status    VARCHAR,
  timestamp VARCHAR
) WITH (
  KAFKA_TOPIC   = '22.payments.processed',
  VALUE_FORMAT  = 'JSON',
  PARTITIONS    = 3
);

-- === DERIVED STREAM: filter high-value orders ===

CREATE STREAM IF NOT EXISTS high_value_orders AS
  SELECT orderId, userId, product, category, totalAmount
  FROM orders_stream
  WHERE totalAmount >= 100.0
  EMIT CHANGES;

-- === TABLE: running count + sum per userId ===

CREATE TABLE IF NOT EXISTS orders_count_by_user AS
  SELECT
    userId,
    COUNT(*)        AS orderCount,
    SUM(totalAmount) AS totalSpent
  FROM orders_stream
  GROUP BY userId
  EMIT CHANGES;

-- === TABLE: tumbling window (1 minute) per category ===

CREATE TABLE IF NOT EXISTS orders_per_category_1min AS
  SELECT
    category,
    COUNT(*)        AS orderCount,
    SUM(totalAmount) AS revenue,
    WINDOWSTART     AS windowStart,
    WINDOWEND       AS windowEnd
  FROM orders_stream
  WINDOW TUMBLING (SIZE 1 MINUTE)
  GROUP BY category
  EMIT CHANGES;

-- === TABLE: hopping window (5 min / advance 1 min) per userId ===

CREATE TABLE IF NOT EXISTS orders_per_user_hopping AS
  SELECT
    userId,
    COUNT(*)    AS orderCount,
    WINDOWSTART AS windowStart,
    WINDOWEND   AS windowEnd
  FROM orders_stream
  WINDOW HOPPING (SIZE 5 MINUTES, ADVANCE BY 1 MINUTE)
  GROUP BY userId
  EMIT CHANGES;

-- === STREAM-TABLE JOIN: enrich orders with user aggregate stats ===

CREATE STREAM IF NOT EXISTS enriched_orders AS
  SELECT
    o.orderId,
    o.userId,
    o.product,
    o.totalAmount,
    u.orderCount   AS userTotalOrders,
    u.totalSpent   AS userTotalSpent
  FROM orders_stream o
  LEFT JOIN orders_count_by_user u ON o.userId = u.userId
  EMIT CHANGES;