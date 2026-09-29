# Branch 21 — Log Compaction

## 1. Що вивчаємо у цій гілці

- **`cleanup.policy=compact`** — Kafka зберігає лише останнє значення per key (замість всіх подій).
- **Tombstone record** — null value → сигнал видалення ключа з compacted topic.
- **`cleanup.policy=delete,compact`** — комбінований режим: і compaction, і time-based retention.
- **Compaction параметри** — `min.cleanable.dirty.ratio`, `segment.bytes`, `delete.retention.ms`.
- **State Reconstruction** — consumer з `auto-offset-reset=earliest` відновлює поточний стан
  зі скомпактованого топіку.
- **Use cases** — каталог продуктів, налаштування, стан сутності (entity changelog).

---

## 2. Зміни порівняно з попередньою гілкою (branch20)

- **Замінено** order-flow на product-catalog сценарій.
- **Нові сервіси**: product-catalog-service та catalog-reader-service (без order-service).
- **Топік** `21.products` з `cleanup.policy=compact` замість звичайного `delete`.
- **Видалено** Kafka Streams — фокус на механізм compaction рівня broker.
- **Новий API**: PUT для оновлення, DELETE для tombstone, `/demo/chaos` для масового тесту.

---

## 3. Архітектура

```
product-catalog-service ──[POST /api/products]──► topic: 21.products (compacted)
                         ──[PUT  /api/products/{id}]──► publish updated product
                         ──[DELETE /api/products/{id}]──► publish TOMBSTONE (null value)

catalog-reader-service ──[21.products]──► in-memory current state
                        ──[GET /api/catalog/current]──► latest value per key
                        ──[GET /api/catalog/stats]──► tombstones, total counts
```

**Як працює compaction:**
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
  (prod-2 видалено через tombstone + delete.retention.ms)
```

**Сервіси та порти (з docker-compose-21.yml):**
- `kafka` — 9092 (KRaft, `KAFKA_LOG_CLEANER_ENABLE=true`)
- `kafka-ui` — 8080 → http://localhost:8080
- `product-catalog-b21` — 8087 → http://localhost:8087
- `catalog-reader-b21` — 8088 → http://localhost:8088

---

## 4. Ключові концепції

### cleanup.policy=compact vs delete

```
cleanup.policy=delete (default):
  Видаляє повідомлення після retention.ms (7 днів за замовч)
  Use case: event log, audit trail, temporal data

cleanup.policy=compact:
  Зберігає ЛИШЕ останнє повідомлення per key
  Старі версії видаляються Log Cleaner-ом
  Use case: changelog, state store, довідники (продукти, конфіги)

cleanup.policy=delete,compact:
  Compaction + видалення після retention.ms
  Use case: entity state з обмеженим терміном зберігання
```

### Параметри compaction топіку

```kotlin
// KafkaTopicConfig.kt
TopicBuilder.name("21.products")
    .partitions(3)
    .replicas(1)
    .config(TopicConfig.CLEANUP_POLICY_CONFIG, "compact")
    .config(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.01") // 1% dirty → агресивно
    .config(TopicConfig.SEGMENT_BYTES_CONFIG, "1048576")           // 1MB (маленький для демо)
    .config(TopicConfig.DELETE_RETENTION_MS_CONFIG, "100")         // tombstone видалити через 100мс
    .build()
```

- `min.cleanable.dirty.ratio=0.01` — compaction запускається коли 1% log dirty (для демо)
- `segment.bytes=1MB` — маленькі сегменти → швидше compaction (в production: 1GB)
- `delete.retention.ms=100` — tombstone видаляється через 100мс (в production: 24h)

### Tombstone record

```kotlin
// product-catalog-service
fun deleteProduct(productId: String) {
    kafkaTemplate.send(ProducerRecord("21.products", productId, null))
    // null value = tombstone
}
```

Після compaction ключ `productId` зникне з топіку (після `delete.retention.ms`).
catalog-reader-service, що читає `auto-offset-reset=earliest`, побачить tombstone,
видалить продукт зі своєї in-memory map.

### State Reconstruction

```kotlin
// catalog-reader-service
@KafkaListener(topics = ["21.products"])
fun handleProduct(record: ConsumerRecord<String, String?>) {
    if (record.value() == null) {
        // tombstone
        currentCatalog.remove(record.key())
        tombstoneCount.incrementAndGet()
    } else {
        currentCatalog[record.key()] = record.value()!!
    }
}
```

При запуску catalog-reader-service читає топік з `earliest`:
- Отримує лише останні версії кожного продукту (compacted)
- Відновлює in-memory стан каталогу

---

## 5. Як запустити

```bash
docker compose -f docker-compose-21.yml up --build

# Kafka UI: http://localhost:8080
```

---

## 6. Як тестувати

### Крок 1 — Seed каталогу

```bash
curl -s -X POST http://localhost:8087/api/products/demo/seed | jq .
# Creates 5 products: prod-1 to prod-5
```

### Крок 2 — Кілька версій одного продукту

```bash
# prod-1: початкова ціна 1000
curl -s http://localhost:8087/api/products/prod-1 | jq .

# Оновити ціну тричі
curl -s -X PUT http://localhost:8087/api/products/prod-1 \
  -H "Content-Type: application/json" -d '{"price":950.0}' | jq .
curl -s -X PUT http://localhost:8087/api/products/prod-1 \
  -H "Content-Type: application/json" -d '{"price":920.0}' | jq .
curl -s -X PUT http://localhost:8087/api/products/prod-1 \
  -H "Content-Type: application/json" -d '{"price":899.0}' | jq .

# Kafka UI: топік 21.products містить 4 записи для prod-1 (до compaction)
# Після compaction залишиться 1 (ціна 899)
```

### Крок 3 — Tombstone

```bash
curl -s -X DELETE http://localhost:8087/api/products/prod-2
# Publishes null value → prod-2 зникне після compaction
```

### Крок 4 — Chaos demo

```bash
curl -s -X POST http://localhost:8087/api/products/demo/chaos | jq .
# 3 продукти × 5 updates + 1 delete
# Ідеально для спостереження compaction
```

### Крок 5 — Поточний стан reader-service

```bash
curl -s http://localhost:8088/api/catalog/current | jq .
# {"prod-1":{"name":"Laptop Pro","price":899.0}, ...}
# prod-2 відсутній (tombstone)

curl -s http://localhost:8088/api/catalog/stats | jq .
# {"totalReceived": N, "tombstones": M, "currentSize": K}
```

### Крок 6 — CLI перегляд compaction

```bash
# Всі повідомлення (включно з tombstones)
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic 21.products \
  --from-beginning \
  --property print.key=true \
  --property print.value=true \
  --timeout-ms 3000

# Конфігурація топіку
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe --topic 21.products
# cleanup.policy=compact, min.cleanable.dirty.ratio=0.01, ...

# Log segments (compaction на рівні файлів)
docker exec kafka ls /var/lib/kafka/data/21.products-0/
```

---

## 7. Поглиблений розгляд

### Як Log Cleaner працює

```
Log = набір Segments (файлів):
  [segment-0.log] [segment-1.log] [active-segment.log]
       (clean)          (dirty)        (active, не компактується)

Log Cleaner:
  1. Визначає dirty ratio: dirty_log_size / total_log_size
  2. Якщо dirty ratio > min.cleanable.dirty.ratio → compaction
  3. Читає dirty segment → будує map {key → latest_offset}
  4. Видаляє записи де не latest offset для свого key
  5. Записує скомпактований segment

Active segment НІКОЛИ не компактується (тільки закриті segments)
```

### Гарантії compaction

```
Compaction НЕ гарантує:
  - Негайне видалення старих версій
  - Що старих версій немає до compaction

Compaction гарантує:
  - Після compaction: для кожного key є ХОЧА Б ОДИН запис (останній)
  - "Tail" (старі segments) скомпактований
  - "Head" (нові записи) ще не скомпактований

Consumer з earliest offset:
  Може побачити і старі версії (якщо ще не скомпактовано)
  Після compaction: лише останню версію кожного ключа
```

### Compacted topic як event sourcing store

```
Щоразу при перезапуску:
  1. catalog-reader-service: auto-offset-reset=earliest
  2. Читає весь compacted topic
  3. Відновлює in-memory стан
  4. Готовий до обробки нових подій

→ Compacted topic = persistent event store для state reconstruction
→ Без потреби у зовнішній БД для поточного стану
```

---

## 8. Структура проекту

```
branch21_log_compaction/
├── product-catalog-service/
│   ├── controller/ProductController.kt   ← POST, PUT, DELETE /api/products
│   │                                        POST /api/products/demo/seed
│   │                                        POST /api/products/demo/chaos
│   ├── service/ProductCatalogService.kt  ← publish to 21.products (with tombstone)
│   └── config/KafkaTopicConfig.kt        ← TopicBuilder з compact policy
└── catalog-reader-service/
    ├── listener/ProductUpdateListener.kt ← @KafkaListener, handles null (tombstone)
    ├── service/CatalogStateService.kt    ← in-memory ConcurrentHashMap
    └── controller/CatalogController.kt   ← GET /api/catalog/current, /stats
```

---

## 9. Що далі

- **Branch 22** — ksqlDB: SQL-рушій поверх Kafka Streams; CREATE STREAM, TABLE, push/pull queries.

---

## 10. Слайди для лекції

### Слайд 1 — delete vs compact

```
cleanup.policy=delete:              cleanup.policy=compact:
  [key=A, v=1]  ← видалиться         [key=A, v=1]  ← видалиться
  [key=A, v=2]  ← видалиться         [key=A, v=2]  ← видалиться
  [key=A, v=3]  ← залишиться 7 днів  [key=A, v=3]  ← ЗАЛИШИТЬСЯ ЗАВЖДИ
  [key=B, v=1]  ← залишиться 7 днів  [key=B, v=1]  ← ЗАЛИШИТЬСЯ ЗАВЖДИ

  Через 7 днів: всі видаляться        Завжди: тільки остання версія per key
```

### Слайд 2 — Tombstone

```
producer.send(key="prod-2", value=null)   ← tombstone

Перед compaction:    Після compaction:    Після delete.retention.ms:
  [prod-2, v=1]        [prod-2, null]       (нічого — ключ видалено)
  [prod-2, v=2]
  [prod-2, null]
```

### Слайд 3 — Use Cases

```
cleanup.policy=delete:        cleanup.policy=compact:
  - Event log                   - User settings
  - Audit trail                 - Product catalog
  - Time-series metrics         - Feature flags
  - Message queue               - Entity state store
  "Що відбулось?"               "Який зараз стан?"
```

---

## 11. Демонстраційний сценарій

```bash
#!/bin/bash
echo "=== Branch 21: Log Compaction Demo ==="

docker compose -f docker-compose-21.yml up --build -d
sleep 60

echo ""
echo "=== Крок 1: Seed каталогу ==="
curl -s -X POST http://localhost:8087/api/products/demo/seed | jq .

echo ""
echo "=== Крок 2: Кілька версій prod-1 ==="
for price in 950 920 899; do
  curl -s -X PUT http://localhost:8087/api/products/prod-1 \
    -H "Content-Type: application/json" -d "{\"price\":$price.0}" | jq .price
done

echo ""
echo "=== Крок 3: Tombstone для prod-2 ==="
curl -s -X DELETE http://localhost:8087/api/products/prod-2
echo "prod-2 видалено (tombstone)"

echo ""
echo "=== Крок 4: Chaos demo ==="
curl -s -X POST http://localhost:8087/api/products/demo/chaos | jq .

echo ""
echo "=== Поточний стан reader ==="
curl -s http://localhost:8088/api/catalog/current | jq .
curl -s http://localhost:8088/api/catalog/stats | jq .

echo ""
echo "=== CLI: конфігурація топіку ==="
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic 21.products
```

---

## 12. Питання для самоперевірки

- Яка різниця між `cleanup.policy=delete` та `cleanup.policy=compact`?
- Що таке tombstone record і коли він видаляється?
- Чому `min.cleanable.dirty.ratio=0.01` для демо, але 0.5 для production?
- Що таке active segment і чому він не компактується?
- Як catalog-reader-service відновлює стан після рестарту?
- Коли компактований топік краще за звичайну реляційну БД для зберігання стану?
- Що відбудеться якщо consumer читає topic під час compaction?
- Навіщо `cleanup.policy=delete,compact` і коли його використовувати?