# Аналіз покриття матеріалу для сертифікації Confluent Certified Developer for Apache Kafka (CCDAK)

> Аналіз базується на офіційній програмі іспиту CCDAK та матеріалах лабораторії (branch01–branch19).

---

## Домени іспиту CCDAK та їх покриття

| # | Домен | Вага | Покриття | Статус |
|---|-------|------|----------|--------|
| 1 | Core Concepts (топіки, партиції, офсети, брокери) | ~20% | branch01–03, 16 | ✅ Добре |
| 2 | Producer API | ~20% | branch01, 07, 12 | ✅ Добре |
| 3 | Consumer API | ~20% | branch04–06, 08 | ✅ Добре |
| 4 | Kafka Streams | ~20% | branch14 | ⚠️ Частково |
| 5 | Kafka Connect | ~10% | branch15 | ⚠️ Частково |
| 6 | Schema Registry & Avro | ~10% | branch11 | ✅ Добре |
| 7 | Security | ~5% | branch18 | ⚠️ Частково |
| 8 | Monitoring & Operations | ~5% | branch17 | ✅ Добре |

---

## ✅ Теми, що добре покриті

### Core Concepts
- Topic, Partition, Offset, Consumer Group — branch01–02
- Message Key → Partition routing (Murmur2 hash) — branch03
- Replication Factor, ISR, Leader Election — branch16
- KRaft mode (без Zookeeper) — branch01
- Retention (`retention.ms`) — branch02
- Naming conventions для топіків — branch02

### Producer API
- `KafkaTemplate.send()`, `@SendResult`, callback — branch01, 07
- `acks=0/1/all` — trade-off між надійністю та latency — branch07
- `enable.idempotence=true`, sequence numbers — branch07, 12
- `linger.ms`, `batch.size` — батчинг для throughput — branch07
- `retries`, `retry.backoff.ms`, `delivery.timeout.ms` — branch07
- `max.in.flight.requests.per.connection` — branch07, 12
- Kafka Transactions: `transactional.id`, `executeInTransaction` — branch12
- Idempotent producer у production — branch19

### Consumer API
- `@KafkaListener`, Consumer Groups, Partition Assignment — branch04
- `RangeAssignor` — branch04
- `CooperativeStickyAssignor`, incremental rebalance — branch08
- `ConsumerSeekAware`, partition callbacks — branch04, 08
- `enable-auto-commit: false`, `ack-mode: MANUAL_IMMEDIATE` — branch06
- `ack.acknowledge()`, `ack.nack(Duration)` — branch06
- `auto-offset-reset: earliest/latest` — branch01, 05
- `max.poll.records`, `max.poll.interval.ms` — branch08
- `session.timeout.ms`, `heartbeat.interval.ms` — branch08
- `isolation-level: read_committed` — branch12
- Static Group Membership (`group.instance.id`) — branch08
- Graceful shutdown — branch08
- Offset reset через CLI `--reset-offsets --to-earliest` — branch05
- Consumer lag — branch05, 17

### Schema Registry & Avro
- Avro `.avsc` схеми, генерація коду через Gradle Plugin — branch11
- Schema ID у кожному повідомленні (5 байт) — branch11
- `kafka-avro-serializer`, `kafka-streams-avro-serde` — branch11, 14
- Backward/Forward/Full/None compatibility — branch11
- Schema Evolution: додавання поля з default — branch11
- REST API Schema Registry — branch11

### Error Handling
- `@RetryableTopic` — non-blocking retry через Kafka топіки — branch09
- Exponential backoff — branch09
- `@DltHandler`, Dead Letter Topic — branch09, 10, 19
- `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` — branch19

### Transactions & EOS
- Exactly-Once Semantics через transactional producer + `read_committed` — branch12
- Abort flow та control batch в Kafka log — branch12

### Kafka Streams
- `KStream`, `KTable` — різниця, семантика — branch14
- `groupBy`, `count`, `aggregate` — branch14
- Tumbling window — branch14
- State stores (RocksDB), Interactive Queries — branch14
- `@EnableKafkaStreams` — branch14

### Kafka Connect
- Source connector (Debezium PostgreSQL CDC) — branch15
- Sink connector (Elasticsearch) — branch15
- SMT `ExtractNewRecordState` — branch15
- Debezium CDC конверт: `before/after/op` — branch15
- Replication slot у PostgreSQL — branch15
- Connect REST API — branch15

### Security
- SASL/PLAIN автентифікація — branch18
- ACL (Access Control Lists): WRITE/READ per principal — branch18
- `deny by default` (`ALLOW_EVERYONE_IF_NO_ACL_FOUND=false`) — branch18
- `TopicAuthorizationException` — branch18
- JAAS конфігурація — branch18

### Observability
- kafka-exporter → Prometheus → Grafana — branch17
- Ключові метрики: `kafka_consumergroup_lag`, `kafka_topic_partition_current_offset` — branch17

### Patterns
- Saga Choreography з компенсацією — branch13, 19
- Type headers (`__TypeId__`) і `spring.json.type.mapping` — branch02, 10
- Event versioning (`eventVersion`) — branch10
- Idempotency Key для захисту від дублювання — branch13

---

## ⚠️ Теми, покриті частково або поверхово

### Kafka Streams (branch14 потребує розширення)
- **Відсутні KStream joins**: `KStream-KTable join`, `KStream-KStream join` — важлива тема іспиту
- **Відсутній GlobalKTable** — читає всі партиції на кожному вузлі (на відміну від KTable)
- **Відсутні Hopping/Session/Sliding windows** — згадані теоретично, але не реалізовані
- **Відсутній Exactly-Once у Kafka Streams** (`processing.guarantee=exactly_once_v2`)
- **Відсутня топологія `punctuate`** — scheduled tasks в Streams процесорі
- **Відсутній `Processor API`** — низькорівневий API (transform, process)

### Kafka Connect (branch15 потребує розширення)
- **Відсутні converters** — `JsonConverter`, `AvroConverter`, `ByteArrayConverter`
- **Відсутні transforms (SMT) різних типів**: `ReplaceField`, `MaskField`, `TimestampConverter`, `InsertField`
- **Відсутній standalone vs distributed mode** — порівняння режимів роботи Connect
- **Відсутня конфігурація `tasks.max`** — паралелізм конекторів
- **Відсутній custom connector** — як реалізувати власний Source/Sink

### Security (branch18 потребує доповнення)
- **Відсутній SSL/TLS** — шифрування трафіку між клієнтами і брокером (тільки SASL без SSL)
- **Відсутній SASL/SCRAM** — більш безпечна альтернатива PLAIN
- **Відсутній SASL/GSSAPI (Kerberos)** — enterprise-рівень автентифікації
- **Відсутній SSL mutual authentication** (mTLS) — клієнтські сертифікати

---

## ❌ Теми, що відсутні в лабораторії (критичні для CCDAK)

### 1. ksqlDB / Confluent SQL для Kafka
**Вага на іспиті: ~10–15% (у Confluent-орієнтованій версії)**
- `CREATE STREAM`, `CREATE TABLE` над Kafka топіками
- `SELECT` з continuous query
- `JOIN` між стрімами і таблицями
- `WINDOW` агрегації у ksqlDB
- Push query vs Pull query
- ksqlDB CLI та REST API

### 2. Log Compaction
**Вага: часто зустрічається в питаннях**
- `cleanup.policy=compact` — зберігати лише останнє значення per key
- `cleanup.policy=delete,compact` — комбінований режим
- `min.cleanable.dirty.ratio`, `segment.ms`, `delete.retention.ms`
- Tombstone record (null value) — видалення ключа з compacted topic
- Порівняння delete vs compact для різних use-cases (event log vs state store)

### 3. Compression
**Вага: присутнє в питаннях про producer оптимізацію**
- `compression.type`: `none`, `gzip`, `snappy`, `lz4`, `zstd`
- Trade-off: розмір повідомлення vs CPU
- Broker-side vs producer-side compression
- `producer` значення — брокер зберігає у форматі producer-а

### 4. Kafka AdminClient API
**Вага: використовується в практичних питаннях**
- Програмне створення топіків (`AdminClient.createTopics()`)
- `DescribeCluster`, `DescribeTopics`, `ListConsumerGroups`
- `AlterConfigs` — зміна конфігурації топіку без рестарту
- Використовується в branch16 лише через REST, не через AdminClient

### 5. MirrorMaker 2 / Cross-cluster Replication
**Вага: є в питаннях про disaster recovery**
- Active-Active vs Active-Passive топологія
- `MirrorSourceConnector`, `MirrorCheckpointConnector`, `MirrorHeartbeatConnector`
- Offset translation між кластерами
- `replication.policy.class`

### 6. Kafka Quotas
**Вага: менша, але зустрічається**
- `producer_byte_rate`, `consumer_byte_rate` — обмеження пропускної здатності per client
- `request_percentage` — CPU-квота
- `kafka-configs --alter --add-config 'producer_byte_rate=1048576'`

### 7. Rack Awareness
**Вага: менша**
- `broker.rack` — розподіл реплік по різних rack/AZ
- `replica.selector.class: RackAwareReplicaSelector`

### 8. Consumer Interceptors / Producer Interceptors
**Вага: зустрічається в питаннях**
- `ProducerInterceptor` — `onSend()`, `onAcknowledgement()`
- `ConsumerInterceptor` — `onConsume()`, `onCommit()`
- Застосування: метрики, трасування, шифрування

### 9. Kafka Streams — Processor API
**Вага: є в advanced питаннях**
- `Topology` + `Processor`, `Transformer`
- `punctuate()` — scheduled callbacks
- `StateStore` у низькорівневому API

### 10. Preferred Replica Election та Partition Reassignment
**Вага: operations-питання**
- `kafka-leader-election --type preferred`
- `kafka-reassign-partitions` — ручне переміщення партицій між брокерами
- `auto.leader.rebalance.enable`

---

## Підсумкова оцінка готовності

| Домен | Готовність | Коментар |
|-------|------------|----------|
| Core Concepts | **90%** | Відмінне покриття |
| Producer API | **85%** | Відсутні: compression, interceptors |
| Consumer API | **90%** | Відмінне покриття |
| Kafka Streams | **50%** | Відсутні: joins, GlobalKTable, EOS, Processor API |
| Kafka Connect | **55%** | Відсутні: converters, tasks.max, standalone mode |
| Schema Registry | **90%** | Відмінне покриття |
| Security | **60%** | Відсутні: SSL/TLS, SCRAM, Kerberos |
| Monitoring | **80%** | Добре, але відсутні JMX метрики |
| **ksqlDB** | **0%** | Не розглянуто взагалі |
| Log Compaction | **0%** | Не розглянуто взагалі |
| AdminClient API | **10%** | Лише CLI, не програмний API |

### Загальна готовність: ~**65–70%**

---

## Рекомендації для досягнення 90%+ готовності

### Пріоритет 1 — Критичні прогалини
1. **branch20** або окремий модуль: **Log Compaction** (`cleanup.policy=compact`, tombstone)
2. **branch21**: **ksqlDB** — CREATE STREAM/TABLE, push/pull queries, JOIN, WINDOW
3. **branch22**: **SSL/TLS** — keystore/truststore, SASL_SSL, mutual TLS

### Пріоритет 2 — Важливі доповнення
4. Розширити **branch14** (Streams): додати KStream joins, GlobalKTable, EOS (`exactly_once_v2`)
5. Розширити **branch15** (Connect): різні SMT, converters, `tasks.max`, standalone mode
6. Окремий модуль: **Compression** — benchmark gzip/lz4/zstd vs none

### Пріоритет 3 — Completion
7. **Kafka AdminClient API** — програмне управління кластером
8. **MirrorMaker 2** — cross-cluster replication
9. **Producer/Consumer Interceptors**
10. **Quotas** — `kafka-configs --alter`

---

> Матеріал лабораторії є відмінною практичною базою для розуміння Kafka.
> Для впевненої здачі CCDAK рекомендується доповнити ksqlDB, Log Compaction, SSL/TLS та розширити Kafka Streams.