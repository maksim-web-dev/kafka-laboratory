# Kafka Laboratory — Зведений огляд всіх гілок

---

## `branch01_basic` — Basic Kafka in Docker

- Мінімальна система: order-service (Producer) → Kafka → notification-service (Consumer)
- Запуск Kafka у **KRaft mode** (без Zookeeper) у Docker Compose
- `KafkaTemplate.send(topic, key, value)` — публікація повідомлень
- `@KafkaListener(topics, groupId)` — підписка на топік
- `spring.json.add.type.headers=false` — серіалізація JSON без type-заголовків
- `JsonDeserializer` з `spring.json.value.default.type` — явне задання типу на consumer-і
- `auto-offset-reset: earliest` — читання з початку при першому старті
- Концепції: Topic, Producer, Consumer, Consumer Group, Offset, Message Key

---

## `branch02_topics_partitions` — Topics & Partitions

- Розширення до 4 топіків з префіксом `02.` (уникнення конфліктів між гілками)
- Топік `02.orders.created` з **3 партиціями** — паралельна обробка
- Топік `02.orders.cancelled` з **1 партицією** — суворий порядок
- Налаштування `retention.ms` (`604_800_000` — 7 днів, `86_400_000` — 1 день)
- `spring.json.add.type.headers=true` — type headers `__TypeId__` у повідомленнях
- `spring.json.type.mapping` — аліаси замість FQCN у заголовку
- Два `@KafkaListener` для двох різних типів подій в одній consumer group
- CLI-команди: `kafka-topics --describe`, `kafka-consumer-groups --describe`
- Концепції: Partition, Partition selection (hash % N), Offset, Retention, Naming conventions

---

## `branch03_keys` — Message Keys

- Зміна ключа з `orderId` (random UUID) на `userId` (детермінований)
- `hash("userId") % numPartitions` — один userId завжди потрапляє в одну партицію
- Демо-ендпоїнт `/demo/keyed` — всі повідомлення одного userId в одній партиції
- Демо-ендпоїнт `/demo/round-robin` — `key=null` → StickyPartitioner
- **StickyPartitioner** (Kafka 2.4+) — null-key повідомлення "прилипають" до партиції в межах batch
- `kafkaTemplate.send(...).get()` — синхронне очікування підтвердження від брокера
- `record.key()` у listener — перегляд ключа маршрутизації
- Концепції: Key → Partition mapping, порядок гарантований в межах партиції, правильний вибір ключа

---

## `branch04_customer_groups` — Consumer Groups

- 3 екземпляри notification-service в одній consumer group — кожен читає 1 партицію
- **RangeAssignor** — явне задання `partition.assignment.strategy`
- **ConsumerSeekAware** — callbacks при rebalance (`onPartitionsAssigned`, `onPartitionsRevoked`)
- `INSTANCE_ID` env var — ідентифікація кожного екземпляра
- Static Group Membership через `group.instance.id` (обмеження: конфлікт при кількох `@KafkaListener`)
- `session.timeout.ms` — час до визнання consumer мертвим
- Демонстрація rebalance: зупинка одного екземпляра → перерозподіл партицій
- CLI: `kafka-consumer-groups --describe` — offset, lag, partition assignment
- Концепції: 1 partition = 1 consumer, RangeAssignor vs RoundRobinAssignor, Parallelism

---

## `branch05_multiple_consumer_groups` — Multiple Consumer Groups

- Два незалежних сервіси читають один топік: `notification-service-group` і `analytics-service-group`
- Кожна consumer group має **власний offset** — читання однією групою не впливає на іншу
- `auto-offset-reset: earliest` — нова group читає з початку топіку
- Новий сервіс `analytics-service` — накопичує статистику per userId
- Демо: зупинка analytics → публікація нових подій → перезапуск → дочитування пропущеного
- Offset reset через CLI: `--reset-offsets --to-earliest --execute` — replay всіх подій
- `__consumer_offsets` — внутрішній топік Kafka зі зберіганням прогресу груп
- CLI: порівняння lag двох груп одночасно
- Концепції: Fan-out, Independent Offsets, Replay, No data loss

---

## `branch06_offsets_commits` — Offsets & Commits

- Перехід з **auto commit** на **manual commit**
- `enable-auto-commit: false` + `ack-mode: manual_immediate`
- `ack.acknowledge()` — явна фіксація offset після успішної обробки
- `ack.nack(Duration)` — відхилення повідомлення з затримкою повторної доставки
- **At-least-once delivery** — збій до `acknowledge()` → перечитування з незафіксованого offset
- **At-most-once** (auto-commit) — ризик втрати при збої між commit і обробкою
- Симуляція збою через `POST /simulate-failure/{count}`
- Лічильник `redelivered` у відповіді `/count`
- Концепції: manual vs auto commit, idempotency при at-least-once

---

## `branch07_producer_configuration` — Producer Configuration

- Три режими `acks`: `0` (fire-and-forget), `1` (leader ack), `all` (all-ISR ack)
- **`enable.idempotence=true`** — sequence numbers, broker відхиляє дублікати при retry
- Батчинг: `linger.ms` (час накопичення) + `batch.size` (максимальний розмір)
- `delivery.timeout.ms` — максимальний час спроб доставки
- `retries` + `retry.backoff.ms` — повторні спроби при мережевих помилках
- `max.in.flight.requests.per.connection` ≤ 5 при `enable.idempotence=true`
- Benchmark-ендпоїнти для порівняння throughput і latency трьох режимів
- `min.insync.replicas` — мінімум ISR для прийняття запису з `acks=all`
- Концепції: надійність vs latency, idempotent producer, batching throughput

---

## `branch08_consumer_configuration` — Consumer Configuration

- **CooperativeStickyAssignor** замість RangeAssignor — incremental rebalance
- **EAGER vs COOPERATIVE**: EAGER зупиняє всю групу; COOPERATIVE — лише партиції що "переїжджають"
- `group.instance.id` per `@KafkaListener` — унікальний static membership для кожного listener
- `max.poll.records` — обмеження розміру batch (захист від перевищення `max.poll.interval`)
- `max.poll.interval.ms` — максимальний час між двома `poll()` до виключення з групи
- `session.timeout.ms` та `heartbeat.interval.ms` — баланс швидкості виявлення збою
- `server.shutdown=graceful` — завершення обробки поточного batch перед зупинкою
- `GET /api/notifications/config` — ендпоїнт з поточною конфігурацією consumer
- Концепції: stop-the-world vs incremental rebalance, graceful shutdown

---

## `branch09_error_retry` — Error Handling & Dead Letter Topic

- **`@RetryableTopic`** — non-blocking retry через окремі Kafka топіки
- Автоматично створювані retry топіки: `-retry-0`, `-retry-1`, `-dlt`
- `backoff = @Backoff(delay = 1000, multiplier = 5.0)` — exponential backoff (1с → 5с)
- **`@DltHandler`** — обробник Dead Letter Topic після вичерпання всіх спроб
- **Non-blocking**: retry повідомлення не блокує обробку нових (на відміну від blocking retry)
- Retry headers: `kafka_original_topic`, `kafka_original_offset`, `kafka_exception-message`
- `DltStrategy.FAIL_ON_ERROR` — DLT handler не повторює
- `TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE` — суфікси `-retry-0`, `-retry-1`
- Новий сервіс `payment-service` замість notification
- Концепції: blocking vs non-blocking retry, DLT visibility, persistent retry state

---

## `branch10_json_serialization` — JSON Serialization

- Повний 3-сервісний pipeline: order → payment → notification
- `spring.json.type.mapping` — аліаси для cross-service десеріалізації без `ClassNotFoundException`
- `spring.json.trusted.packages` — whitelist пакетів для безпечної десеріалізації
- `spring.json.add.type.headers: true` — `__TypeId__` header з alias (не FQCN)
- **Event versioning** — поле `eventVersion: "1.0"` для backward compatibility
- **Nested objects** — `List<OrderItem>` всередині `OrderCreatedEvent`
- `@DltHandler` публікує `PaymentProcessedEvent(status=DECLINED)` замість тихого логування
- Новий топік `10.payments.processed` — payment-service як producer
- Концепції: type mapping per service, versioning, multi-topic pipeline

---

## `branch11_schema_registry` — Schema Registry & Avro

- Confluent **Schema Registry** — центральне сховище схем, версіонування, перевірка сумісності
- **Apache Avro** — бінарна серіалізація, `.avsc` схеми, генерація Java-класів через Gradle Avro Plugin
- Schema ID (4 байти) вбудований у кожне Avro повідомлення — consumer отримує схему динамічно
- Avro розмір (~80 байт) vs JSON (~300 байт) для `OrderCreatedEvent`
- **Backward compatibility** — додання поля з `default` приймається реєстром
- **Incompatible change** — видалення поля без default → `error_code: 409`
- Режими сумісності: `BACKWARD`, `FORWARD`, `FULL`, `NONE`
- Schema Registry REST API: `/subjects`, `/versions/latest`, `/compatibility/...`
- Перевірка нової схеми перед реєстрацією через `POST /compatibility/...`
- Стек: `kafka-avro-serializer 7.7.1`, `com.github.davidmc24.gradle.plugin.avro 1.9.1`

---

## `branch12_idempotent_producer` — Idempotent Producer & Transactions

- **`enable.idempotence=true`** — producer ID + sequence number, broker відхиляє дублікати
- Вимоги idempotence: `acks=all`, `retries > 0`, `max.in.flight.requests ≤ 5`
- **Kafka Transactions** — `transactional.id`, `beginTransaction` / `commitTransaction` / `abortTransaction`
- `transaction-id-prefix` — Spring Kafka автоматично створює `KafkaTransactionManager`
- `kafkaTemplate.executeInTransaction { }` — атомарне відправлення
- **`isolation-level: read_committed`** — consumer бачить лише committed повідомлення
- `read_uncommitted` vs `read_committed` — ізоляція транзакцій на рівні consumer
- Abort marker у Kafka log — фізично присутній, але `read_committed` consumer ігнорує
- Демо: abort транзакції → notification-service не отримує повідомлення
- Концепції: Exactly-Once Semantics (EOS), commit/abort flow

---

## `branch13_saga_pattern` — Saga Pattern (Choreography)

- **Choreography Saga** — сервіси реагують на події без центрального оркестра
- Happy path (5 кроків): orders.created → payments.processed → inventory.reserved → orders.confirmed
- **Compensating Transactions**: inventory.failed → refund (payment) + cancel (order)
- **Idempotency Key** — поле `idempotencyKey` для захисту від дублювання при retry
- 4 сервіси: order-service, payment-service, inventory-service, notification-service
- 8 топіків для різних станів і компенсацій
- Нові Avro-схеми: `PaymentFailedEvent`, `InventoryReservedEvent`, `OrderConfirmedEvent` та інші
- Choreography vs Orchestration: слабка зв'язність vs centralized state, SPOF
- Концепції: розподілені транзакції без 2PC, компенсація, відсутність SPOF

---

## `branch14_streams` — Kafka Streams

- **KStream** — необмежений потік подій, обробка record-by-record
- **KTable** — changelog stream, зберігає останній стан per key (результат `count()`)
- `groupBy(userId).count()` — rolling count per userId → KTable + state store
- **Tumbling window** (1 хвилина) — count per category без overlap
- **Tumbling window** (5 хвилин) — aggregate сума продажів per userId
- **State stores** — RocksDB-backed сховища, доступні через Interactive Queries REST API
- `StoreQueryParameters.fromNameAndType("store-name", keyValueStore())` — прямий доступ до store
- `@EnableKafkaStreams` — Spring Kafka інтеграція
- Filter high-value orders → окремий output топік
- Типи вікон: Tumbling, Hopping, Session, Sliding
- Залежності: `kafka-streams`, `kafka-streams-avro-serde`

---

## `branch15_connect_cdc_debezium` — Kafka Connect: CDC з Debezium та Elasticsearch

- **Kafka Connect** — фреймворк інтеграції Kafka без написання коду
- **CDC (Change Data Capture)** — захоплення змін з PostgreSQL через WAL (Write-Ahead Log)
- **Debezium** PostgreSQL source connector — `plugin.name: pgoutput`, replication slot
- Debezium конверт: `before`, `after`, `op` (c/u/d/r), `ts_ms`
- **Elasticsearch Sink connector** — зберігання CDC-подій у search-індексі
- **SMT ExtractNewRecordState** — розгортання Debezium конверта, залишає лише `after`-стан
- `connect-init` контейнер — автоматична реєстрація конекторів після готовності Connect
- Типи операцій CDC: `c` (INSERT), `u` (UPDATE), `d` (DELETE), `r` (snapshot)
- Kafka Connect REST API: `/connectors`, `/connectors/{name}/status`
- Kibana Data Views для перегляду CDC-даних
- Order-service не знає про Kafka — тільки PostgreSQL

---

## `branch16_cluster_replication` — Cluster & Replication

- **3-broker cluster** у KRaft mode в одному Docker Compose
- **Replication Factor=3** — кожна партиція має 3 копії на різних брокерах
- **ISR (In-Sync Replicas)** — підмножина реплік що синхронізовані з лідером
- **Leader Election** — автоматичне перепризначення лідера при зупинці брокера
- `min.insync.replicas=2` — мінімум ISR для прийняття запису з `acks=all`
- Демо 1 (3/3 ISR): нормальна робота
- Демо 2 (2/3 ISR): один брокер зупинений — система продовжує (2 ≥ min.isr)
- Демо 3 (1/3 ISR): два брокери зупинені — `NotEnoughReplicasException`
- Демо 4: відновлення брокерів → ISR відновлюється
- REST API для перегляду стану кластера: `/api/cluster/brokers`, `/api/cluster/topic-info`
- Рекомендації: dev rf=1, staging rf=2, production rf=3/min.isr=2

---

## `branch17_observability` — Observability: Prometheus + Grafana

- **kafka-exporter** — збір Kafka метрик у форматі Prometheus (`:9308`)
- **Prometheus** — збір та зберігання метрик
- **Grafana** — дашборди з автопровізією через provisioning
- 7 панелей дашборду: Consumer Lag, Messages in Topic, Consumer Offset, Active Brokers, Lag Over Time, Producer vs Consumer Offset, Production Rate
- Ключові метрики: `kafka_consumergroup_lag`, `kafka_topic_partition_current_offset`, `kafka_brokers`
- `max.poll.records=1` — зміна lag видна пообіцянково (per message)
- `max.poll.interval.ms=600000` — consumer в pause не виключається з групи
- API управління consumer: `pause`, `resume`, `slow?ms=N`, `fast`
- Ендпоїнт `/api/orders/flood?count=N` — масова генерація повідомлень для спостереження lag

---

## `branch18_security` — Security: SASL/PLAIN + ACL

- **SASL/PLAIN автентифікація** — кожен сервіс має власний логін/пароль
- `KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL: PLAIN`
- `KAFKA_AUTHORIZER_CLASS_NAME: org.apache.kafka.metadata.authorizer.StandardAuthorizer`
- `KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND: false` — deny by default
- `KAFKA_SUPER_USERS: User:admin` — superuser обходить ACL
- **ACL (Access Control Lists)** — WRITE/READ/Describe per principal per resource
- ACL для consumer group: `User:notification-consumer` → `group:notification-service-group` → Read
- `KAFKA_JAAS` конфігурація через `kafka_server_jaas.conf`
- Spring Boot SASL конфіг: `security.protocol`, `sasl.mechanism`, `sasl.jaas.config`
- `acl-init` контейнер — одноразовий: створює топік + налаштовує ACL
- Демо ACL violation: `notification-consumer` намагається писати → `TopicAuthorizationException`

---

## `branch19_production` — Production-like Demo

- Повна production-ready система, що об'єднує паттерни з усіх попередніх гілок
- **3-broker cluster** з `replication-factor=3`, `min.insync.replicas=2`
- **SASL/PLAIN + ACL** для всіх 4 сервісів (принцип least privilege)
- **Idempotent producer** (`enable.idempotence=true`) в order-service та payment-service
- **Saga Choreography** з компенсацією (payment fail → cancel, inventory fail → refund + cancel)
- **Manual offset commit** (`AckMode.MANUAL_IMMEDIATE`) в inventory-service
- **Dead Letter Topic** з `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` в notification-service
- **Avro + Schema Registry** — строга типізація для всіх подій
- **`isolation.level=read_committed`** — notification-service ігнорує незавершені транзакції
- **Prometheus + Grafana** — consumer lag alerting, DLT counter
- `AUTO_CREATE_TOPICS_ENABLE=false` — топіки тільки через `init.sh`
- 9 топіків з prefixом `19.`, включно з `19.orders.created.dlt`
- Production checklist покриває всі аспекти: reliability, security, observability, exactly-once