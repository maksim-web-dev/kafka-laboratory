# Branch 15 — Kafka Connect: CDC з Debezium та Elasticsearch

## Що вивчаємо

- **Kafka Connect** — фреймворк для інтеграції Kafka з зовнішніми системами без написання коду
- **CDC (Change Data Capture)** — захоплення змін з БД через WAL (Write-Ahead Log)
- **Debezium** — source connector для PostgreSQL CDC
- **Elasticsearch sink** — зберігання CDC-подій у пошуковому індексі
- **SMT (Single Message Transforms)** — `ExtractNewRecordState` для розгортання Debezium-конверта

## Архітектура

```
order-service ──► PostgreSQL (WAL)
                       │
                  Debezium CDC
                       │
                       ▼
                 Kafka topic:
              postgres.public.orders
                       │
              Elasticsearch Sink
                       │
                       ▼
               Elasticsearch index
                       │
                    Kibana
```

**Ключовий момент:** `order-service` не знає про Kafka взагалі — він просто зберігає дані в PostgreSQL. Debezium автоматично фіксує кожну зміну через WAL і публікує в Kafka.

## Сервіси та порти

| Сервіс | Порт | URL |
|--------|------|-----|
| Kafka (external) | 9116 | - |
| Kafka UI | 8140 | http://localhost:8140 |
| PostgreSQL | 5416 | jdbc:postgresql://localhost:5416/ordersdb |
| Kafka Connect REST API | 8161 | http://localhost:8161 |
| Elasticsearch | 9216 | http://localhost:9216 |
| Kibana | 5616 | http://localhost:5616 |
| order-service | 8162 | http://localhost:8162 |

## Запуск

```bash
docker compose -f docker-compose-15.yml up --build
```

> ⚠️ Перший запуск займає ~5 хвилин: `kafka-connect` завантажує Debezium та Elasticsearch плагіни через `confluent-hub install`.

Контейнер `connect-init` автоматично реєструє обидва конектори після того, як Kafka Connect стане готовим.

## Перевірка конекторів

```bash
# Список зареєстрованих конекторів
curl http://localhost:8161/connectors

# Статус Debezium source
curl http://localhost:8161/connectors/orders-source-connector/status

# Статус Elasticsearch sink
curl http://localhost:8161/connectors/orders-sink-connector/status
```

## Демо

### 1. Створити замовлення (одне)

```bash
curl -X POST http://localhost:8162/api/orders \
  -H "Content-Type: application/json" \
  -d '{"userId": "alice", "totalAmount": 149.99, "category": "ELECTRONICS"}'
```

### 2. Створити пакет замовлень

```bash
curl -X POST "http://localhost:8162/api/orders/batch?users=alice,bob,carol&count=10"
```

### 3. Переглянути замовлення в PostgreSQL (через API)

```bash
curl http://localhost:8162/api/orders
```

### 4. Переглянути Kafka-топік у Kafka UI

Відкрий http://localhost:8140 → Topics → `postgres.public.orders`

Кожен запис має формат Debezium CDC:
```json
{
  "before": null,
  "after": {
    "id": "...",
    "user_id": "alice",
    "total_amount": 149.99,
    "category": "ELECTRONICS",
    "status": "PENDING",
    "created_at": "..."
  },
  "op": "c",
  "ts_ms": 1234567890
}
```

### 5. Переглянути дані в Elasticsearch

```bash
# Кількість документів
curl http://localhost:9216/postgres.public.orders/_count

# Пошук всіх замовлень
curl "http://localhost:9216/postgres.public.orders/_search?pretty"

# Пошук по userId
curl "http://localhost:9216/postgres.public.orders/_search?pretty" \
  -H "Content-Type: application/json" \
  -d '{"query": {"match": {"user_id": "alice"}}}'
```

### 6. Kibana

Відкрий http://localhost:5616 → Management → Kibana → Data Views → Create data view:
- Name: `orders`
- Index pattern: `postgres.public.orders`

## Типи операцій CDC (поле `op`)

| Код | Операція | Коли |
|-----|----------|------|
| `c` | CREATE | INSERT в таблицю |
| `u` | UPDATE | UPDATE запису |
| `d` | DELETE | DELETE запису |
| `r` | READ | Snapshot при старті Debezium |

## Конфігурація конекторів

### Debezium Source (`connect/debezium-source.json`)

```json
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "topic.prefix": "postgres",
  "table.include.list": "public.orders",
  "plugin.name": "pgoutput"
}
```

- `topic.prefix` → Kafka топік: `postgres.public.orders`
- `plugin.name: pgoutput` → вбудований в PostgreSQL 10+ replication plugin
- `slot.name` → PostgreSQL replication slot (зберігає позицію читання WAL)

### Elasticsearch Sink (`connect/elasticsearch-sink.json`)

```json
{
  "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
  "topics": "postgres.public.orders",
  "transforms": "unwrap",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState"
}
```

- `ExtractNewRecordState` SMT → розгортає Debezium-конверт, залишаючи тільки `after`-стан
- `key.ignore: true` → не використовує Kafka message key як ES document id
- `schema.ignore: true` → не вимагає JSON Schema в повідомленнях

## Ручна реєстрація конекторів (якщо connect-init не спрацював)

```bash
# Source connector
curl -X POST http://localhost:8161/connectors \
  -H "Content-Type: application/json" \
  --data @branch15_connect_cdc_debezium/connect/debezium-source.json

# Sink connector
curl -X POST http://localhost:8161/connectors \
  -H "Content-Type: application/json" \
  --data @branch15_connect_cdc_debezium/connect/elasticsearch-sink.json
```

## Зупинка

```bash
docker compose -f docker-compose-15.yml down
```