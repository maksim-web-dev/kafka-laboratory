# Branch 18 — Security: SASL/PLAIN + ACL

## 1. Що вивчаємо у цій гілці

- **SASL/PLAIN аутентифікація** — логін/пароль для кожного Kafka клієнта (user/password).
- **ACL (Access Control Lists)** — декларативні правила: хто може читати/писати який топік.
- **`StandardAuthorizer`** — вбудований Kafka авторизатор для KRaft режиму.
- **`TopicAuthorizationException`** — що отримує клієнт при порушенні ACL.
- **`ALLOW_EVERYONE_IF_NO_ACL_FOUND=false`** — deny by default (найбезпечніша стратегія).
- **Super Users** — `User:admin` обходить всі ACL перевірки.
- **`acl-init` контейнер** — одноразовий контейнер що створює топіки та налаштовує ACL.

---

## 2. Зміни порівняно з попередньою гілкою (branch17)

- **Видалено** Prometheus, Grafana, kafka-exporter — фокус на security.
- **Додано** SASL/PLAIN конфігурацію до kafka брокера.
- **Додано** `acl-init-b18` — одноразовий контейнер для ініціалізації ACL.
- **Змінено** Spring Kafka config: додано `security.protocol`, `sasl.mechanism`, `sasl.jaas.config`.
- **Додано** `AclProbeController` у notification-service для демонстрації ACL violation.
- **Kafka listener** змінено: `PLAINTEXT:SASL_PLAINTEXT` замість `PLAINTEXT:PLAINTEXT`.

---

## 3. Архітектура

```
order-service (user: order-producer)
  SASL_PLAINTEXT ──WRITE──► Kafka (SASL + ACL) ──► 18.orders.created
                                                         │
notification-service (user: notification-consumer)       │
  SASL_PLAINTEXT ──READ────────────────────────────────◄─┘
```

**SASL Users та ACL:**
- `order-producer` / `order-secret` — WRITE до `18.orders.created`
- `notification-consumer` / `notif-secret` — READ з `18.orders.created`
- `admin` / `admin-secret` — Super User (обходить ACL)

**Сервіси та порти (з docker-compose-18.yml):**
- `kafka` — 9092 (SASL_PLAINTEXT, KRaft)
- `kafka-ui` — 8080 → http://localhost:8080
- `acl-init-b18` — одноразовий (без port)
- `order-service-b18` — 8081 → http://localhost:8081
- `notification-service-b18` — 8082 → http://localhost:8082

---

## 4. Ключові концепції

### SASL/PLAIN конфігурація брокера

```yaml
# docker-compose-18.yml — kafka environment
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:SASL_PLAINTEXT
KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL: PLAIN
KAFKA_SASL_ENABLED_MECHANISMS: PLAIN
KAFKA_AUTHORIZER_CLASS_NAME: org.apache.kafka.metadata.authorizer.StandardAuthorizer
KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND: "false"
KAFKA_SUPER_USERS: "User:admin"
KAFKA_OPTS: "-Djava.security.auth.login.config=/etc/kafka/secrets/kafka_server_jaas.conf"
```

**kafka_server_jaas.conf:**
```
KafkaServer {
  org.apache.kafka.common.security.plain.PlainLoginModule required
  username="admin"
  password="admin-secret"
  user_admin="admin-secret"
  user_order-producer="order-secret"
  user_notification-consumer="notif-secret";
};
```

### Spring Boot SASL конфігурація

```yaml
# order-service application.yml
spring:
  kafka:
    properties:
      security.protocol: SASL_PLAINTEXT
      sasl.mechanism: PLAIN
      sasl.jaas.config: >
        org.apache.kafka.common.security.plain.PlainLoginModule required
        username="order-producer" password="order-secret";
```

### ACL правила (acl-init.sh)

```bash
# Створити топік (від імені admin)
kafka-topics --bootstrap-server kafka:9092 \
  --command-config /etc/kafka/secrets/admin.conf \
  --create --topic 18.orders.created --partitions 3 --replication-factor 1

# ACL для order-producer (WRITE)
kafka-acls --bootstrap-server kafka:9092 \
  --command-config /etc/kafka/secrets/admin.conf \
  --add --allow-principal User:order-producer \
  --operation Write --operation Describe \
  --topic 18.orders.created

# ACL для notification-consumer (READ)
kafka-acls --bootstrap-server kafka:9092 \
  --command-config /etc/kafka/secrets/admin.conf \
  --add --allow-principal User:notification-consumer \
  --operation Read --operation Describe \
  --topic 18.orders.created

kafka-acls --bootstrap-server kafka:9092 \
  --command-config /etc/kafka/secrets/admin.conf \
  --add --allow-principal User:notification-consumer \
  --operation Read \
  --group notification-service-group
```

### TopicAuthorizationException

```kotlin
// AclProbeController у notification-service
// notification-consumer намагається WRITE → заборонено ACL

fun attemptWrite(): Map<String, String> {
    return try {
        kafkaTemplate.send("18.orders.created", "probe", "test").get()
        mapOf("result" to "ALLOWED")
    } catch (e: ExecutionException) {
        val cause = e.cause
        if (cause is TopicAuthorizationException) {
            mapOf(
                "result" to "DENIED",
                "exception" to "TopicAuthorizationException",
                "message" to cause.message!!
            )
        } else throw e
    }
}
```

---

## 5. Як запустити

```bash
docker compose -f docker-compose-18.yml up --build

# Порядок запуску:
# 1. kafka (з SASL) → healthy
# 2. acl-init → створює топік + ACL → exits successfully
# 3. order-service, notification-service (depends on acl-init)

# Kafka UI: http://localhost:8080
```

**Перевірка:**
```bash
# Список ACL (від імені admin)
docker exec acl-init-b18 kafka-acls \
  --bootstrap-server kafka:9092 \
  --command-config /etc/kafka/secrets/admin.conf \
  --list
```

---

## 6. Як тестувати

### Тест 1 — Нормальна відправка (order-producer → WRITE ok)

```bash
curl -X POST "http://localhost:8081/api/orders?userId=user-1&totalAmount=199.9&category=ELECTRONICS"
# 200 OK, orderId=...
# Лог order-service: [SEC] Sent orderId=...
# Лог notification-service: Received orderId=...
```

### Тест 2 — ACL violation (notification-consumer → WRITE заборонено)

```bash
curl -X POST http://localhost:8082/api/probe/write-attempt | jq .
# {
#   "result": "DENIED",
#   "exception": "TopicAuthorizationException",
#   "message": "Not authorized to access topics: [18.orders.created]",
#   "explanation": "notification-consumer has READ ACL only"
# }
```

### Тест 3 — Підключення без пароля

```bash
# Спробуємо підключитись без SASL credentials
kafka-topics --bootstrap-server localhost:9092 --list
# SASL authentication failed: Authentication failed due to invalid credentials
```

### Тест 4 — Переглянути ACL через CLI

```bash
docker exec acl-init-b18 kafka-acls \
  --bootstrap-server kafka:9092 \
  --command-config /etc/kafka/secrets/admin.conf \
  --list --topic 18.orders.created
# Current ACLs for resource `ResourcePattern(resourceType=TOPIC, name=18.orders.created, ...)`:
#   (principal=User:order-producer, host=*, operation=WRITE, permissionType=ALLOW)
#   (principal=User:notification-consumer, host=*, operation=READ, permissionType=ALLOW)
```

---

## 7. Поглиблений розгляд

### SASL vs SSL

```
SASL/PLAIN (branch18):
  Хто ти? → логін/пароль
  Транспорт: PLAINTEXT (трафік НЕ зашифрований!)
  Протокол: SASL_PLAINTEXT
  Use case: внутрішня мережа за firewall

SSL (branch23):
  Шифрування: TLS (трафік зашифрований)
  Аутентифікація: опціонально через клієнтські сертифікати (mTLS)
  Протокол: SSL або SASL_SSL

Production: SASL_SSL = SASL аутентифікація + SSL шифрування (branch23)
```

### Deny by Default

```yaml
KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND: "false"
```

Без ACL для ресурсу → **заборонено**. Це принцип least privilege.
Якщо `true` → без ACL дозволено всім (менш безпечно).

### acl-init.sh pattern

```
Проблема: order-service та notification-service мають стартувати
ПІСЛЯ того як топік та ACL створені.

Рішення:
  acl-init (one-shot container) → creates topics + ACLs → exits(0)
  order-service depends_on acl-init: condition: service_completed_successfully
  notification-service depends_on acl-init: condition: service_completed_successfully
```

### Kafka UI SASL конфігурація

```yaml
# docker-compose-18.yml — kafka-ui environment
KAFKA_CLUSTERS_17_PROPERTIES_SECURITY_PROTOCOL: SASL_PLAINTEXT
KAFKA_CLUSTERS_17_PROPERTIES_SASL_MECHANISM: PLAIN
KAFKA_CLUSTERS_17_PROPERTIES_SASL_JAAS_CONFIG: >
  org.apache.kafka.common.security.plain.PlainLoginModule required
  username="admin" password="admin-secret";
```

Kafka UI підключається як admin → бачить всі топіки.

---

## 8. Структура проекту

```
branch18_security/
├── kafka/
│   ├── kafka_server_jaas.conf  ← SASL users: admin, order-producer, notification-consumer
│   ├── admin.conf              ← admin client properties для kafka CLI
│   └── acl-init.sh             ← створює топік 18.orders.created + ACL
├── order-service/
│   ├── controller/OrderController.kt   ← POST /api/orders (WRITE to Kafka)
│   └── config/KafkaProducerConfig.kt   ← SASL_PLAINTEXT producer
└── notification-service/
    ├── controller/AclProbeController.kt ← POST /api/probe/write-attempt
    ├── listener/OrderEventListener.kt   ← READ from Kafka
    └── config/KafkaConsumerConfig.kt    ← SASL_PLAINTEXT consumer
```

---

## 9. Що далі

- **Branch 19** — Production-like: SASL/PLAIN + 3-broker cluster + Avro + Saga + Monitoring.
- **Branch 23** — SSL/TLS: SASL_SSL = SASL + SSL шифрування (повноцінний production security).

---

## 10. Слайди для лекції

### Слайд 1 — SASL/PLAIN Flow

```
Client (order-producer)             Kafka Broker
  │                                      │
  ├── TCP connect ────────────────────► │
  ├── SaslHandshake(PLAIN) ───────────► │
  ├── username=order-producer ─────────► │
  │   password=order-secret             │
  │                              verify credentials
  │                              check StandardAuthorizer
  │◄──── ALLOWED (if credentials ok AND ACL permits) ──┤
```

### Слайд 2 — ACL Matrix

```
              18.orders.created
              WRITE   READ   GROUP READ
admin         ✓       ✓      ✓  (super user)
order-producer  ✓       ✗      ✗
notification  ✗       ✓      ✓ (notification-service-group)

ALLOW_EVERYONE_IF_NO_ACL_FOUND=false → all else DENIED
```

### Слайд 3 — SASL_PLAINTEXT vs SASL_SSL

```
SASL_PLAINTEXT:
  Аутентифікація ✓ (пароль перевіряється)
  Шифрування    ✗ (трафік у відкритому вигляді)
  Use case: internal network only

SASL_SSL:
  Аутентифікація ✓
  Шифрування    ✓ (TLS)
  Use case: public network, production
```

---

## 11. Демонстраційний сценарій

```bash
#!/bin/bash
echo "=== Branch 18: Security SASL/ACL Demo ==="

docker compose -f docker-compose-18.yml up --build -d
sleep 60

echo ""
echo "=== ACL список ==="
docker exec acl-init-b18 kafka-acls \
  --bootstrap-server kafka:9092 \
  --command-config /etc/kafka/secrets/admin.conf \
  --list

echo ""
echo "=== Тест 1: Авторизований запис (order-producer) ==="
curl -s -X POST "http://localhost:8081/api/orders?userId=user-1&totalAmount=99.9&category=FOOD" | jq .

echo ""
echo "=== Тест 2: ACL порушення (notification-consumer намагається писати) ==="
curl -s -X POST http://localhost:8082/api/probe/write-attempt | jq .

echo ""
echo "=== Тест 3: Batch надсилання ==="
for i in $(seq 1 5); do
  curl -s -X POST "http://localhost:8081/api/orders?userId=user-$i&totalAmount=$((i*50)).0&category=ELECTRONICS" | jq .orderId
done

echo ""
echo "=== Kafka UI (admin): http://localhost:8080 ==="
echo "Можна переглядати топіки і повідомлення"
```

---

## 12. Питання для самоперевірки

- Яка різниця між SASL_PLAINTEXT і SASL_SSL?
- Що таке ACL і де вони зберігаються в Kafka (KRaft)?
- Що означає `ALLOW_EVERYONE_IF_NO_ACL_FOUND=false`?
- Чому `User:admin` обходить ACL навіть якщо немає явного дозволу?
- Яку операцію треба дозволити consumer group читати топік?
- Що таке `kafka_server_jaas.conf` і для чого він використовується?
- Навіщо acl-init запускається як окремий контейнер (не через entrypoint kafka)?
- Як Kafka UI підключається до SASL-захищеного брокера?