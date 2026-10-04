# Branch 15 — Kafka Connect: CDC with Debezium and Elasticsearch

## 1. What we learn in this branch

- **Kafka Connect** — framework for integrating Kafka with external systems without writing
  consumer/producer code.
- **CDC (Change Data Capture)** — capturing every database change through WAL (Write-Ahead Log)
  instead of polling.
- **Debezium** — CDC Source Connector for PostgreSQL: reads WAL, publishes to Kafka.
- **Elasticsearch Sink Connector** — stores CDC events in a search index.
- **SMT (Single Message Transform)** — `ExtractNewRecordState` unwraps the Debezium envelope,
  keeping only the `after` state.
- **`connect-init`** — initializer service that automatically registers connectors via REST API.

---

## 2. Changes compared to the previous branch (branch14)

- **No Kafka producer** is used in order-service — it simply writes to PostgreSQL.
- **Kafka Connect** replaces the manual producer for getting data into Kafka.
- **Added** PostgreSQL (with WAL replication mode), Elasticsearch, Kibana.
- **Removed** Avro / Schema Registry — Kafka Connect uses JSON converter.
- **order-service** is now a Spring Data JPA application with no Kafka dependencies.

---

## 3. Architecture

```
order-service (Spring JPA)
      │  INSERT/UPDATE/DELETE
      ▼
PostgreSQL (WAL: wal_level=logical)
      │
      │  Debezium Source Connector
      │  (reads replication slot)
      ▼
Kafka topic: postgres.public.orders
      │
      │  Elasticsearch Sink Connector
      │  + SMT ExtractNewRecordState
      ▼
Elasticsearch index: postgres.public.orders
      │
      ▼
Kibana (search/visualization)
```

**Key point:** order-service **does not know** about Kafka. It only saves data to PostgreSQL.
Debezium automatically captures every change through WAL.

**Services and ports (from docker-compose-15.yml):**
- `kafka` — 9092 (KRaft single-node)
- `kafka-ui` — 8080 → http://localhost:8080
- `postgres-b15` — 5432 → `jdbc:postgresql://localhost:5432/ordersdb`
- `kafka-connect-b15` — 8083 → http://localhost:8083
- `elasticsearch-b15` — 9200 → http://localhost:9200
- `kibana-b15` — 5601 → http://localhost:5601
- `order-service-b15` — 8081 → http://localhost:8081
- `connect-init-b15` — one-time container (registers connectors)

---

## 4. Key Concepts

### CDC via PostgreSQL WAL

```
WAL (Write-Ahead Log) — journal of all changes in PostgreSQL for crash recovery.
Debezium reads this journal via Logical Replication (pgoutput plugin).

PostgreSQL settings for CDC:
  wal_level=logical          ← enable logical replication
  max_replication_slots=4    ← number of slots for reading WAL
  max_wal_senders=4          ← number of processes transmitting WAL

Debezium creates a replication slot (stores WAL read position):
  slot.name=debezium
```

### Kafka Connect REST API

```
Kafka Connect is managed via REST:

GET  /connectors                    → list connectors
POST /connectors                    → register a new connector
GET  /connectors/{name}/status      → connector state (RUNNING/FAILED/PAUSED)
DELETE /connectors/{name}           → delete connector

The Kafka Connect Worker process runs connectors as tasks in its JVM.
```

### Debezium CDC Envelope

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
- `before` = state before the operation (null for INSERT)
- `after` = state after the operation

### SMT ExtractNewRecordState

```json
{
  "transforms": "unwrap",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState"
}
```

Without SMT, Elasticsearch receives the full Debezium envelope.
After SMT — only fields from `after`:
```json
{"id":"550e8400...","user_id":"alice","total_amount":149.99,"status":"PENDING"}
```

### Connector Configuration

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

## 5. How to Run

```bash
# First run takes ~5 minutes:
# kafka-connect installs plugins via confluent-hub install
docker compose -f docker-compose-15.yml up --build

# connect-init automatically registers both connectors
# Kafka UI: http://localhost:8080
# Kibana: http://localhost:5601
```

**Readiness check:**
```bash
# Both connectors must be RUNNING
curl http://localhost:8083/connectors/orders-source-connector/status | jq .connector.state
curl http://localhost:8083/connectors/orders-sink-connector/status | jq .connector.state
# "RUNNING"
```

**If connect-init did not work (manual registration):**
```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  --data @branch15_connect_cdc_debezium/connect/debezium-source.json

curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  --data @branch15_connect_cdc_debezium/connect/elasticsearch-sink.json
```

---

## 6. How to Test

### Step 1 — Create an order

```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId": "alice", "totalAmount": 149.99, "category": "ELECTRONICS"}'
```

### Step 2 — Check the Kafka topic

```bash
# Kafka UI: http://localhost:8080 → Topics → postgres.public.orders
# Or via CLI:
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic postgres.public.orders \
  --from-beginning --max-messages 1 | jq .
# Shows Debezium envelope with op="c"
```

### Step 3 — Check Elasticsearch

```bash
# Document count
curl http://localhost:9200/postgres.public.orders/_count | jq .count

# Search all
curl "http://localhost:9200/postgres.public.orders/_search?pretty&size=3" | jq .hits.hits

# Search by userId
curl "http://localhost:9200/postgres.public.orders/_search?pretty" \
  -H "Content-Type: application/json" \
  -d '{"query":{"match":{"user_id":"alice"}}}' | jq .hits.total.value
```

### Step 4 — Batch and UPDATE

```bash
# Batch of 10 orders
curl -X POST "http://localhost:8081/api/orders/batch?users=alice,bob,carol&count=10"

# After an UPDATE in PostgreSQL (if API exists) — Kafka will receive op="u" with before and after
```

### Step 5 — Kibana

```
Open: http://localhost:5601
Management → Stack Management → Kibana → Data Views → Create data view
  Name: orders
  Index pattern: postgres.public.orders
  → Save
Discover → select data view "orders" → see all documents
```

---

## 7. Deep Dive

### Why CDC is better than polling?

```
Polling (traditional approach):
  SELECT * FROM orders WHERE updated_at > last_check_time
  Problem: requires an updated_at column, does not capture DELETE,
            loads the DB, possible missed changes on race condition

CDC via WAL:
  Reads the physical DB journal — ALL operations (INSERT/UPDATE/DELETE)
  Independent of table schema
  Zero overhead on DB (WAL is written regardless)
  Guaranteed event order
```

### Replication Slot

```
Replication Slot — a PostgreSQL mechanism that:
  1. Stores the WAL read position (LSN — Log Sequence Number)
  2. Guarantees WAL is not deleted until the slot has read it
  3. Debezium uses the slot for checkpointing

Danger: if Debezium is stopped for too long — WAL accumulates
(disk overflow!). In production, monitor pg_replication_slots.
```

### Kafka Connect Worker vs Task

```
Kafka Connect Worker (JVM process):
  Executes Tasks internally
  Can run multiple connectors simultaneously

Task — actual unit of work:
  Debezium Source: 1 task (reads 1 replication slot)
  Elasticsearch Sink: N tasks (one per partition/group)

Distributed mode (multiple Workers):
  Tasks are distributed between Workers automatically
  → horizontal scaling of Connect
```

---

## 8. Project Structure

```
branch15_connect_cdc_debezium/
├── connect/
│   ├── Dockerfile                    ← base CP Kafka Connect + confluent-hub install
│   ├── debezium-source.json          ← Debezium PostgreSQL connector config
│   └── elasticsearch-sink.json       ← Elasticsearch sink connector config
└── order-service/
    ├── controller/OrderController.kt ← REST API (no Kafka)
    ├── service/OrderService.kt        ← save to PostgreSQL
    ├── repository/OrderRepository.kt  ← Spring Data JPA
    └── model/Order.kt                 ← @Entity for ordersdb.public.orders
```

---

## 9. What's Next

- **Branch 16** — Multi-broker cluster: 3 Kafka brokers, Replication Factor, ISR, leader election,
  `NotEnoughReplicasException`.

---

## 10. Lecture Slides

### Slide 1 — Kafka Connect vs Custom Code

```
Custom producer/consumer:
  Write code → compile → test → deploy → maintain

Kafka Connect:
  Configure JSON → POST to REST API → RUNNING

Connect = reusable connectors for 200+ systems
(PostgreSQL, MySQL, MongoDB, S3, Elasticsearch, BigQuery, ...)
```

### Slide 2 — CDC vs Polling

```
Polling (every 5 sec):            CDC (WAL-based):
  SELECT WHERE updated > last →    INSERT → WAL → Debezium → Kafka
  - requires updated_at column     - ALL operations automatically
  - does not capture DELETE        - including DELETE
  - loads DB                       - zero overhead
  - may miss changes               - guaranteed order
```

### Slide 3 — SMT ExtractNewRecordState

```
Without SMT:                      After SMT:
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

## 11. Demo Script

```bash
#!/bin/bash
echo "=== Branch 15: Kafka Connect CDC Demo ==="

docker compose -f docker-compose-15.yml up --build -d
echo "Waiting 5 minutes (loading plugins)..."
sleep 300

echo ""
echo "=== Check connectors ==="
curl -s http://localhost:8083/connectors/orders-source-connector/status | jq .connector.state
curl -s http://localhost:8083/connectors/orders-sink-connector/status | jq .connector.state

echo ""
echo "=== Step 1: Create an order ==="
curl -s -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId":"alice","totalAmount":149.99,"category":"ELECTRONICS"}' | jq .

echo ""
echo "=== Step 2: Kafka UI → postgres.public.orders ==="
echo "Open: http://localhost:8080"
sleep 3

echo ""
echo "=== Step 3: Check Elasticsearch ==="
curl -s http://localhost:9200/postgres.public.orders/_count | jq .count

echo ""
echo "=== Step 4: Batch — 10 orders ==="
curl -s -X POST "http://localhost:8081/api/orders/batch?users=alice,bob,carol&count=10"
sleep 3
curl -s http://localhost:9200/postgres.public.orders/_count | jq .count

echo ""
echo "=== Kibana: http://localhost:5601 ==="
echo "Create Data View: postgres.public.orders"
```

---

## 12. Self-Check Questions

- What is WAL and why is it used for CDC?
- Why does PostgreSQL need the parameter `wal_level=logical`?
- What is the difference between CDC and polling for capturing DB changes?
- What is a replication slot and what is the risk of slot lag accumulation?
- What does `op: "u"` mean in a Debezium CDC envelope?
- Why is SMT ExtractNewRecordState needed?
- How does a Kafka Connect Worker differ from a Task?
- How does connect-init register connectors? Can this be done manually?