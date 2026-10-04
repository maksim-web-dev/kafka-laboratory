---
marp: true
theme: default
paginate: true
backgroundColor: #ffffff
style: |
  section {
    font-family: 'Segoe UI', sans-serif;
    font-size: 28px;
  }
  section.title {
    text-align: center;
    justify-content: center;
  }
  h1 { color: #1a1a2e; }
  h2 { color: #16213e; border-bottom: 2px solid #0f3460; padding-bottom: 8px; }
  code { background: #f4f4f4; padding: 2px 6px; border-radius: 4px; }
  pre { background: #1e1e1e; color: #d4d4d4; border-radius: 8px; }
  blockquote { border-left: 4px solid #0f3460; padding-left: 16px; color: #555; }
  .note { font-size: 20px; color: #666; font-style: italic; }
---

<!-- _class: title -->

# Branch 15
# Kafka Connect: CDC with Debezium

**Apache Kafka for Certification & Production**

---

## Agenda

1. Kafka Connect — what it is and why it matters
2. Custom code vs Kafka Connect — comparison
3. CDC vs Polling — why WAL-based CDC wins
4. PostgreSQL WAL — the foundation of Debezium
5. Debezium CDC envelope format
6. SMT `ExtractNewRecordState` — unwrapping the envelope
7. Elasticsearch Sink Connector
8. `connect-init` — automatic connector registration
9. Kafka Connect REST API
10. Key Takeaways & What's Next

---

## What is Kafka Connect?

**A framework for integrating Kafka with external systems — without writing producer/consumer code:**

```
Without Kafka Connect:
  Write producer code → compile → test → deploy → maintain
  Write consumer code → compile → test → deploy → maintain
  Repeat for every new data source or sink

With Kafka Connect:
  Configure JSON → POST to REST API → connector is RUNNING
  No code to write, no code to maintain
  Handles: error handling, offset management, retries, monitoring
```

<br>

- 200+ ready-made connectors: PostgreSQL, MySQL, MongoDB, S3, Elasticsearch, BigQuery...
- **Source Connector** — reads from external system, writes to Kafka
- **Sink Connector** — reads from Kafka, writes to external system

> Connect = reusable integration without boilerplate code.

---

## Custom Code vs Kafka Connect

**Why reinvent when a connector already exists?**

```
Custom Kafka producer for DB changes:
  SELECT WHERE updated_at > last_poll_time
  requires updated_at column in every table
  misses DELETE operations entirely
  adds polling load to the DB
  may miss fast changes between polls
  you maintain all the logic

Debezium Source Connector (CDC):
  reads PostgreSQL WAL (Write-Ahead Log)
  captures ALL changes: INSERT, UPDATE, DELETE
  zero overhead on application tables
  guaranteed ordering per table
  battle-tested, maintained by community
  you just configure JSON
```

> If a connector exists — use it. Don't build what doesn't need building.

---

## CDC vs Polling

**Change Data Capture using the Write-Ahead Log:**

```
Polling (every 5 seconds):
  SELECT * FROM orders WHERE updated_at > :last_check
  - requires updated_at column on every table
  - DELETE is invisible (row gone, no way to detect)
  - DB receives extra load from polling queries
  - may miss rapid changes between polls
  - time window: ±5 seconds accuracy

CDC via WAL (Debezium):
  reads PostgreSQL replication slot
  - ALL operations captured automatically: INSERT, UPDATE, DELETE
  - zero load on application tables
  - exact ordering guaranteed
  - sub-millisecond latency
  - no schema changes required
```

---

## PostgreSQL WAL — The Foundation

**WAL (Write-Ahead Log) = the source of truth for all DB changes:**

```
PostgreSQL writes every change to WAL before committing:
  INSERT → WAL record → data file
  UPDATE → WAL record → data file
  DELETE → WAL record → data file

Debezium uses Logical Replication to read WAL:
  wal_level=logical          ← enable logical decoding
  max_replication_slots=4    ← slots hold WAL position for readers
  max_wal_senders=4          ← parallel WAL stream connections

Debezium creates a replication slot: "debezium"
  → slot remembers last-read WAL position
  → if Debezium restarts → continues from that position (no gaps)
```

> WAL-based CDC = exactly-once capture of every DB change.

---

## Debezium CDC Envelope Format

**Every captured change is wrapped in a structured envelope:**

```json
{
  "before": null,
  "after": {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "user_id": "alice",
    "total_amount": 149.99,
    "status": "PENDING",
    "created_at": 1696350012345
  },
  "op": "c",
  "ts_ms": 1696350012345,
  "source": {"db": "ordersdb", "table": "orders", "lsn": 12345}
}
```

- `op: "c"` = INSERT (create), `"u"` = UPDATE, `"d"` = DELETE, `"r"` = READ (snapshot)
- `before` = state before change (null for INSERT)
- `after` = state after change (null for DELETE)

---

## SMT — ExtractNewRecordState

**Single Message Transform: unwrap the Debezium envelope**

```
Without SMT — Elasticsearch receives full envelope (bloated):
{
  "before": null,
  "after": {"id": "...", "user_id": "alice", "total_amount": 149.99},
  "op": "c", "ts_ms": 1696350012345, "source": {...}
}

After SMT ExtractNewRecordState — only the "after" fields:
{
  "id": "550e8400-...",
  "user_id": "alice",
  "total_amount": 149.99,
  "status": "PENDING"
}
```

```json
"transforms": "unwrap",
"transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState"
```

> SMT transforms messages **inside the connector** — no extra service needed.

---

## Elasticsearch Sink Connector

**Kafka → Elasticsearch: searchable real-time index:**

```json
{
  "name": "orders-elasticsearch-sink",
  "config": {
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "tasks.max": "1",
    "topics": "postgres.public.orders",
    "connection.url": "http://elasticsearch-b15:9200",
    "type.name": "_doc",
    "key.ignore": "false",
    "schema.ignore": "true",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState"
  }
}
```

- Each Kafka message → one Elasticsearch document
- Kafka message key = Elasticsearch document ID (enables UPSERT)
- SMT applied **before** writing to Elasticsearch

---

## connect-init — Automatic Connector Registration

**Registering connectors at startup without manual steps:**

```yaml
connect-init-b15:
  image: curlimages/curl
  depends_on:
    kafka-connect-b15:
      condition: service_healthy
  command: >
    sh -c "
      curl -s -X POST http://kafka-connect-b15:8083/connectors \
        -H 'Content-Type: application/json' \
        -d @/config/debezium-source.json &&
      curl -s -X POST http://kafka-connect-b15:8083/connectors \
        -H 'Content-Type: application/json' \
        -d @/config/elasticsearch-sink.json
    "
```

- `connect-init` is a one-shot container — runs once, registers connectors, exits
- No manual curl commands needed after `docker compose up`

> Automate connector registration — treat connectors as infrastructure-as-code.

---

## Kafka Connect REST API

**All management through HTTP — no CLI tools needed:**

```bash
# List all registered connectors
GET  http://localhost:8083/connectors

# Register a new connector
POST http://localhost:8083/connectors
     Body: {"name": "...", "config": {...}}

# Check connector status
GET  http://localhost:8083/connectors/orders-source-connector/status
# → {"state": "RUNNING", "tasks": [{"state": "RUNNING"}]}

# Pause a connector
PUT  http://localhost:8083/connectors/orders-source-connector/pause

# Delete a connector
DELETE http://localhost:8083/connectors/orders-source-connector
```

> Kafka Connect is fully managed via REST — CI/CD friendly.

---

## Key Takeaways

- **Kafka Connect** = integration without writing producer/consumer code
- **CDC (Debezium)** = capture every INSERT/UPDATE/DELETE via PostgreSQL WAL
- **WAL-based CDC** beats polling: captures DELETEs, zero DB overhead, exact ordering
- **Debezium envelope** contains `before`, `after`, `op` — full change context
- **SMT `ExtractNewRecordState`** — unwrap envelope, keep only `after` state
- **Replication slot** = WAL position bookmark — no gaps on restart
- **`connect-init`** = infrastructure-as-code for connector registration
- **REST API** = full connector lifecycle management

<br>

> order-service does NOT know Kafka exists. Debezium captures everything silently.

---

## What's Next — Branch 16

**Cluster & Replication — Kafka in high-availability mode**

- Multi-broker Kafka cluster (3 brokers in KRaft mode)
- Replication Factor — RF=3, partition replicas across brokers
- Leader election — which broker handles reads and writes
- `min.insync.replicas` under real broker failure scenarios
- Producer behavior during broker failover

---

<!-- _class: title -->

## What's Next — Branch 16
- Multi-broker cluster: RF=3, leader + 2 followers
- Leader election on broker failure
- min.insync.replicas under real failure
- Producer failover behavior

# Questions?

**Branch 15 — Kafka Connect: CDC with Debezium**