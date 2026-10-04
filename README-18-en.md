# Branch 18 — Security: SASL/PLAIN + ACL

## 1. What we learn in this branch

- **SASL/PLAIN authentication** — username/password for every Kafka client.
- **ACL (Access Control Lists)** — declarative rules: who can read/write which topic.
- **`StandardAuthorizer`** — built-in Kafka authorizer for KRaft mode.
- **`TopicAuthorizationException`** — what a client receives when it violates an ACL.
- **`ALLOW_EVERYONE_IF_NO_ACL_FOUND=false`** — deny by default (safest strategy).
- **Super Users** — `User:admin` bypasses all ACL checks.
- **`acl-init` container** — one-shot container that creates topics and configures ACLs.

---

## 2. Changes compared to the previous branch (branch17)

- **Removed** Prometheus, Grafana, kafka-exporter — focus is on security.
- **Added** SASL/PLAIN configuration to the Kafka broker.
- **Added** `acl-init-b18` — one-shot container for ACL initialization.
- **Changed** Spring Kafka config: added `security.protocol`, `sasl.mechanism`, `sasl.jaas.config`.
- **Added** `AclProbeController` in notification-service to demonstrate ACL violation.
- **Kafka listener** changed: `PLAINTEXT:SASL_PLAINTEXT` instead of `PLAINTEXT:PLAINTEXT`.

---

## 3. Architecture

```
order-service (user: order-producer)
  SASL_PLAINTEXT ──WRITE──► Kafka (SASL + ACL) ──► 18.orders.created
                                                         │
notification-service (user: notification-consumer)       │
  SASL_PLAINTEXT ──READ────────────────────────────────◄─┘
```

**SASL Users and ACL:**
- `order-producer` / `order-secret` — WRITE to `18.orders.created`
- `notification-consumer` / `notif-secret` — READ from `18.orders.created`
- `admin` / `admin-secret` — Super User (bypasses ACL)

**Services and ports (from docker-compose-18.yml):**
- `kafka` — 9092 (SASL_PLAINTEXT, KRaft)
- `kafka-ui` — 8080 → http://localhost:8080
- `acl-init-b18` — one-shot (no port)
- `order-service-b18` — 8081 → http://localhost:8081
- `notification-service-b18` — 8082 → http://localhost:8082

---

## 4. Key Concepts

### SASL/PLAIN broker configuration

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

### Spring Boot SASL configuration

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

### ACL rules (acl-init.sh)

```bash
# Create topic (as admin)
kafka-topics --bootstrap-server kafka:9092 \
  --command-config /etc/kafka/secrets/admin.conf \
  --create --topic 18.orders.created --partitions 3 --replication-factor 1

# ACL for order-producer (WRITE)
kafka-acls --bootstrap-server kafka:9092 \
  --command-config /etc/kafka/secrets/admin.conf \
  --add --allow-principal User:order-producer \
  --operation Write --operation Describe \
  --topic 18.orders.created

# ACL for notification-consumer (READ)
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
// AclProbeController in notification-service
// notification-consumer attempts WRITE → denied by ACL

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

## 5. How to Run

```bash
docker compose -f docker-compose-18.yml up --build

# Startup order:
# 1. kafka (with SASL) → healthy
# 2. acl-init → creates topic + ACL → exits successfully
# 3. order-service, notification-service (depends on acl-init)

# Kafka UI: http://localhost:8080
```

**Verification:**
```bash
# List ACLs (as admin)
docker exec acl-init-b18 kafka-acls \
  --bootstrap-server kafka:9092 \
  --command-config /etc/kafka/secrets/admin.conf \
  --list
```

---

## 6. How to Test

### Test 1 — Normal send (order-producer → WRITE ok)

```bash
curl -X POST "http://localhost:8081/api/orders?userId=user-1&totalAmount=199.9&category=ELECTRONICS"
# 200 OK, orderId=...
# order-service log: [SEC] Sent orderId=...
# notification-service log: Received orderId=...
```

### Test 2 — ACL violation (notification-consumer → WRITE denied)

```bash
curl -X POST http://localhost:8082/api/probe/write-attempt | jq .
# {
#   "result": "DENIED",
#   "exception": "TopicAuthorizationException",
#   "message": "Not authorized to access topics: [18.orders.created]",
#   "explanation": "notification-consumer has READ ACL only"
# }
```

### Test 3 — Connect without password

```bash
# Try connecting without SASL credentials
kafka-topics --bootstrap-server localhost:9092 --list
# SASL authentication failed: Authentication failed due to invalid credentials
```

### Test 4 — View ACLs via CLI

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

## 7. Deep Dive

### SASL vs SSL

```
SASL/PLAIN (branch18):
  Who are you? → username/password
  Transport: PLAINTEXT (traffic is NOT encrypted!)
  Protocol: SASL_PLAINTEXT
  Use case: internal network behind a firewall

SSL (branch23):
  Encryption: TLS (traffic is encrypted)
  Authentication: optionally via client certificates (mTLS)
  Protocol: SSL or SASL_SSL

Production: SASL_SSL = SASL authentication + SSL encryption (branch23)
```

### Deny by Default

```yaml
KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND: "false"
```

Without an ACL for a resource → **denied**. This is the principle of least privilege.
If `true` → without ACL everyone is allowed (less secure).

### acl-init.sh pattern

```
Problem: order-service and notification-service must start
AFTER the topic and ACLs have been created.

Solution:
  acl-init (one-shot container) → creates topics + ACLs → exits(0)
  order-service depends_on acl-init: condition: service_completed_successfully
  notification-service depends_on acl-init: condition: service_completed_successfully
```

### Kafka UI SASL configuration

```yaml
# docker-compose-18.yml — kafka-ui environment
KAFKA_CLUSTERS_17_PROPERTIES_SECURITY_PROTOCOL: SASL_PLAINTEXT
KAFKA_CLUSTERS_17_PROPERTIES_SASL_MECHANISM: PLAIN
KAFKA_CLUSTERS_17_PROPERTIES_SASL_JAAS_CONFIG: >
  org.apache.kafka.common.security.plain.PlainLoginModule required
  username="admin" password="admin-secret";
```

Kafka UI connects as admin → can see all topics.

---

## 8. Project Structure

```
branch18_security/
├── kafka/
│   ├── kafka_server_jaas.conf  ← SASL users: admin, order-producer, notification-consumer
│   ├── admin.conf              ← admin client properties for Kafka CLI
│   └── acl-init.sh             ← creates topic 18.orders.created + ACL
├── order-service/
│   ├── controller/OrderController.kt   ← POST /api/orders (WRITE to Kafka)
│   └── config/KafkaProducerConfig.kt   ← SASL_PLAINTEXT producer
└── notification-service/
    ├── controller/AclProbeController.kt ← POST /api/probe/write-attempt
    ├── listener/OrderEventListener.kt   ← READ from Kafka
    └── config/KafkaConsumerConfig.kt    ← SASL_PLAINTEXT consumer
```

---

## 9. What's Next

- **Branch 19** — Production-like: SASL/PLAIN + 3-broker cluster + Avro + Saga + Monitoring.
- **Branch 23** — SSL/TLS: SASL_SSL = SASL + SSL encryption (full production security).

---

## 10. Lecture Slides

### Slide 1 — SASL/PLAIN Flow

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

### Slide 2 — ACL Matrix

```
              18.orders.created
              WRITE   READ   GROUP READ
admin           ✓       ✓      ✓  (super user)
order-producer  ✓       ✗      ✗
notification    ✗       ✓      ✓ (notification-service-group)

ALLOW_EVERYONE_IF_NO_ACL_FOUND=false → all else DENIED
```

### Slide 3 — SASL_PLAINTEXT vs SASL_SSL

```
SASL_PLAINTEXT:
  Authentication ✓ (password verified)
  Encryption     ✗ (traffic in plaintext)
  Use case: internal network only

SASL_SSL:
  Authentication ✓
  Encryption     ✓ (TLS)
  Use case: public network, production
```

---

## 11. Demo Script

```bash
#!/bin/bash
echo "=== Branch 18: Security SASL/ACL Demo ==="

docker compose -f docker-compose-18.yml up --build -d
sleep 60

echo ""
echo "=== ACL list ==="
docker exec acl-init-b18 kafka-acls \
  --bootstrap-server kafka:9092 \
  --command-config /etc/kafka/secrets/admin.conf \
  --list

echo ""
echo "=== Test 1: Authorized write (order-producer) ==="
curl -s -X POST "http://localhost:8081/api/orders?userId=user-1&totalAmount=99.9&category=FOOD" | jq .

echo ""
echo "=== Test 2: ACL violation (notification-consumer attempts write) ==="
curl -s -X POST http://localhost:8082/api/probe/write-attempt | jq .

echo ""
echo "=== Test 3: Batch send ==="
for i in $(seq 1 5); do
  curl -s -X POST "http://localhost:8081/api/orders?userId=user-$i&totalAmount=$((i*50)).0&category=ELECTRONICS" | jq .orderId
done

echo ""
echo "=== Kafka UI (admin): http://localhost:8080 ==="
echo "Can view topics and messages"
```

---

## 12. Self-Check Questions

- What is the difference between SASL_PLAINTEXT and SASL_SSL?
- What are ACLs and where are they stored in Kafka (KRaft)?
- What does `ALLOW_EVERYONE_IF_NO_ACL_FOUND=false` mean?
- Why does `User:admin` bypass ACLs even when there is no explicit permission?
- Which operation must be granted for a consumer group to read a topic?
- What is `kafka_server_jaas.conf` and what is it used for?
- Why does acl-init run as a separate container (not through the Kafka entrypoint)?
- How does Kafka UI connect to a SASL-secured broker?