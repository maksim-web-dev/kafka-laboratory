# Branch 21 — Log Compaction

## Що вивчаємо

| Концепція | Деталі |
|-----------|--------|
| **`cleanup.policy=compact`** | Kafka зберігає лише останнє значення per key |
| **Tombstone record** | Null value → видалення ключа з compacted topic |
| **`cleanup.policy=delete,compact`** | Комбінований режим: і retention, і compaction |
| **Compaction параметри** | `min.cleanable.dirty.ratio`, `segment.ms`, `delete.retention.ms` |
| **Use cases** | Каталог продуктів, налаштування користувачів, стан сутності |

## Коли compaction vs delete

```
cleanup.policy=delete (default):
  Повідомлення видаляються після retention.ms (7 днів)
  Use case: event log, audit trail, часові ряди

cleanup.policy=compact:
  Зберігається лише ОСТАННЄ повідомлення per key
  Старі версії видаляються (compaction log cleaner)
  Use case: changelog, state store, довідники (продукти, користувачі)

cleanup.policy=delete,compact (комбінований):
  Compaction + видалення старих після retention.ms
  Use case: state з обмеженим терміном зберігання
```

## Як працює compaction

```
Топік 21.products (ПЕРЕД compaction):
  key=prod-1, value={"name":"Laptop","price":1000}   offset=0
  key=prod-2, value={"name":"Phone","price":500}      offset=1
  key=prod-1, value={"name":"Laptop","price":950}     offset=2  ← update
  key=prod-3, value={"name":"Tablet","price":300}     offset=3
  key=prod-2, value=null                              offset=4  ← tombstone (delete)
  key=prod-1, value={"name":"Laptop Pro","price":920} offset=5  ← update

Топік 21.products (ПІСЛЯ compaction):
  key=prod-1, value={"name":"Laptop Pro","price":920} offset=5  ← тільки остання версія
  key=prod-3, value={"name":"Tablet","price":300}     offset=3
  (prod-2 видалено через tombstone)
```

## Архітектура

```
product-catalog-service ──[POST /api/products]──► topic: 21.products (compacted)
                         ──[PUT  /api/products/{id}]──► publish updated product
                         ──[DELETE /api/products/{id}]──► publish TOMBSTONE (null value)

catalog-reader-service ──[21.products]──► in-memory current state
                        ──[GET /api/catalog/current]──► latest value per key
```

## Порти

| Сервіс | Порт |
|--------|------|
| Kafka (external) | 9126 |
| Kafka UI | 8201 |
| product-catalog-service | 8202 |
| catalog-reader-service | 8203 |

## Запуск

```bash
docker compose -f docker-compose-21.yml up --build
```

---

## Ключові налаштування топіку

```kotlin
// KafkaTopicConfig.kt
TopicBuilder.name("21.products")
    .partitions(3)
    .replicas(1)
    .config(TopicConfig.CLEANUP_POLICY_CONFIG, "compact")
    .config(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.01")  // compact aggressively
    .config(TopicConfig.SEGMENT_BYTES_CONFIG, "1048576")            // 1MB segments (small for demo)
    .config(TopicConfig.DELETE_RETENTION_MS_CONFIG, "100")          // tombstone visible briefly
    .build()
```

| Параметр | Значення демо | Пояснення |
|----------|---------------|-----------|
| `cleanup.policy` | `compact` | Зберігати лише останній record per key |
| `min.cleanable.dirty.ratio` | `0.01` | 1% dirty → запустити cleaner (дуже агресивно для демо) |
| `segment.bytes` | `1048576` | 1MB segment (маленький для швидшого compaction) |
| `delete.retention.ms` | `100` | Tombstone видаляється через 100мс (для демо) |
| `min.compaction.lag.ms` | не задано | Мінімальний час до compaction (за замовч 0) |

---

## Як протестувати

### 1. Seed — заповнити каталог

```bash
curl -s -X POST http://localhost:8202/api/products/demo/seed | jq
# Creates 5 products: prod-1 to prod-5
```

### 2. Оновити продукт (кілька версій)

```bash
# prod-1: початкова ціна 1000
curl -s http://localhost:8202/api/products/prod-1 | jq

# Оновити ціну тричі
curl -s -X PUT http://localhost:8202/api/products/prod-1 \
  -H "Content-Type: application/json" \
  -d '{"price":950.0}' | jq
curl -s -X PUT http://localhost:8202/api/products/prod-1 \
  -H "Content-Type: application/json" \
  -d '{"price":920.0}' | jq
curl -s -X PUT http://localhost:8202/api/products/prod-1 \
  -H "Content-Type: application/json" \
  -d '{"price":899.0}' | jq

# У Kafka UI топік 21.products містить 4 записи для prod-1
# Після compaction залишиться тільки 1 (остання ціна = 899)
```

### 3. Tombstone — видалити продукт

```bash
curl -s -X DELETE http://localhost:8202/api/products/prod-2
# Publishes null value (tombstone) → після compaction prod-2 зникне
```

### 4. Chaos demo — багато версій → compaction

```bash
curl -s -X POST http://localhost:8202/api/products/demo/chaos | jq
# Creates 3 products, updates each 5 times, deletes 1
# Perfect for observing compaction behavior
```

### 5. Перевірити поточний стан reader-service

```bash
curl -s http://localhost:8203/api/catalog/current | jq
# Shows only LATEST value per key (in-memory state after consuming)

curl -s http://localhost:8203/api/catalog/stats | jq
# {"totalReceived": N, "tombstones": M, "currentSize": K}
```

### 6. CLI — переглянути compaction

```bash
# Показати всі повідомлення (включно з tombstones)
docker exec kafka-b21 kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic 21.products \
  --from-beginning \
  --property print.key=true \
  --property print.value=true \
  --timeout-ms 3000

# Перевірити конфігурацію топіку
docker exec kafka-b21 kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe --topic 21.products
# Output: cleanup.policy=compact, min.cleanable.dirty.ratio=0.01, ...

# Переглянути log segments (compaction відбувається на рівні файлів)
docker exec kafka-b21 ls /var/lib/kafka/data/21.products-0/
```

---

## Ключові концепції

| Концепція | Що демонструє |
|-----------|---------------|
| **`cleanup.policy=compact`** | Kafka broker видаляє старі версії ключа, залишаючи лише останню |
| **Tombstone** | `null` value → ключ видаляється після `delete.retention.ms` |
| **Log Cleaner** | Фоновий процес брокера що виконує compaction по сегментах |
| **Dirty ratio** | відношення "dirty" log (нові записи) до "clean" log (вже компактований) |
| **State reconstruction** | Consumer з `auto-offset-reset=earliest` відновлює поточний стан |
| **At-least-once compaction** | Compaction гарантує що останній record per key збережено |
| **Segment-based** | Compaction працює по log segments — active segment НЕ компактується |