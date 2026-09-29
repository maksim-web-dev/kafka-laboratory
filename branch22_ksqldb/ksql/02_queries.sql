-- ============================================================
-- Branch 22 — ksqlDB: Example Queries
-- Connect to CLI first:
--   docker exec -it ksqldb-cli-b22 ksql http://ksqldb-server-b22:8088
-- ============================================================

-- === SERVICE DISCOVERY ===

SHOW STREAMS;
SHOW TABLES;
SHOW QUERIES;

DESCRIBE orders_stream EXTENDED;
DESCRIBE orders_count_by_user EXTENDED;

-- === PUSH QUERIES (continuous, real-time) ===

-- Всі нові замовлення в реальному часі
SELECT * FROM orders_stream EMIT CHANGES LIMIT 10;

-- Тільки дорогі замовлення (>= 100)
SELECT orderId, userId, product, totalAmount
FROM high_value_orders
EMIT CHANGES LIMIT 5;

-- Live лічильник per user
SELECT userId, orderCount, totalSpent
FROM orders_count_by_user
EMIT CHANGES;

-- Tumbling window: count per category (оновлюється щохвилини)
SELECT category, orderCount, revenue, WINDOWSTART, WINDOWEND
FROM orders_per_category_1min
EMIT CHANGES LIMIT 10;

-- === PULL QUERIES (point-in-time, як SELECT без EMIT CHANGES) ===

-- Скільки замовлень і скільки витратив user-1?
SELECT * FROM orders_count_by_user WHERE userId = 'user-1';

-- Отримати конкретне замовлення (якщо є key)
-- SELECT * FROM orders_stream WHERE orderId = 'some-uuid-here';

-- === STREAM-TABLE JOIN live ===

SELECT orderId, userId, totalAmount, userTotalOrders, userTotalSpent
FROM enriched_orders
EMIT CHANGES LIMIT 5;

-- === CUSTOM AD-HOC QUERIES ===

-- Топ categories по виручці (поточний стан)
SELECT category, SUM(revenue) AS totalRevenue
FROM orders_per_category_1min
GROUP BY category
EMIT CHANGES LIMIT 5;

-- Замовлення за конкретний товар
SELECT orderId, userId, totalAmount
FROM orders_stream
WHERE product = 'Laptop'
EMIT CHANGES LIMIT 5;