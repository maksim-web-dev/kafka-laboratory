---
marp: true
theme: default
paginate: true
backgroundColor: #ffffff
style: |
  section { font-family: 'Segoe UI', sans-serif; font-size: 28px; }
  section.title { text-align: center; justify-content: center; }
  h1 { color: #1a1a2e; }
  h2 { color: #16213e; border-bottom: 2px solid #0f3460; padding-bottom: 8px; }
  code { background: #f4f4f4; padding: 2px 6px; border-radius: 4px; }
  pre { background: #1e1e1e; color: #d4d4d4; border-radius: 8px; }
  blockquote { border-left: 4px solid #0f3460; padding-left: 16px; color: #555; }
---

<!-- _class: title -->

# Branch 18
# Security: SASL/PLAIN + ACL

**Apache Kafka for Certification & Production**

---

## Agenda

1. Why Kafka security matters — default is open
2. SASL/PLAIN — username/password authentication
3. SASL/PLAIN flow — handshake protocol
4. ACL (Access Control Lists) — authorization rules
5. `ALLOW_EVERYONE_IF_NO_ACL_FOUND=false` — deny by default
6. Super Users — admin bypasses all ACLs
7. `acl-init` container — automated setup
8. `TopicAuthorizationException` — violation demo
9. SASL_PLAINTEXT vs SASL_SSL
10. Key Takeaways & What's Next

---

## Why Kafka Security Matters

**Default Kafka: no authentication, no authorization:**

```
Default configuration (PLAINTEXT):
  Any client with network access can:
    ✗ Read any topic
    ✗ Write to any topic
    ✗ Delete any topic
    ✗ Alter consumer group offsets

  This is acceptable for local development.
  This is NEVER acceptable in production.

Branch18 adds:
  ✓ Authentication: username + password per client (SASL/PLAIN)
  ✓ Authorization: per-topic, per-user ACL rules
  ✓ Deny by default: no ACL = no access
```

---

## SASL/PLAIN — Authentication Flow

**Client must provide credentials before any Kafka operation:**

```
Client (order-service)              Kafka Broker
  │                                      │
  ├── TCP connect ─────────────────────► │
  ├── SaslHandshake(PLAIN) ────────────► │
  ├── username=order-producer ──────────► │
  │   password=order-secret              │
  │                               verify credentials
  │                               check StandardAuthorizer
  │                               check ACL for requested operation
  │◄── ALLOWED ────────────────────────── │
  │   (credentials valid AND ACL permits)
  │
  ▼ (connection established, can publish/consume)
```

> Without valid credentials → `SaslAuthenticationException` → connection refused.

---

## ACL Matrix — Who Can Do What

**Principle of least privilege — each client gets only what it needs:**

```
User                 Topic                    WRITE   READ   GROUP
admin (super user)   all topics               ✓       ✓      ✓
order-producer       18.orders.created        ✓       ✗      ✗
notification-cons.   18.orders.created        ✗       ✓      ✓ (notif-group)

ALLOW_EVERYONE_IF_NO_ACL_FOUND=false
  → any access not explicitly ALLOWED is DENIED
```

<br>

**Users and credentials:**

```
order-producer       / order-secret   → WRITE to 18.orders.created
notification-consumer / notif-secret  → READ  from 18.orders.created
admin                / admin-secret   → Super User (all access)
```

---

## Broker SASL Configuration

**Kafka server JAAS and environment settings:**

```yaml
# docker-compose-18.yml — kafka environment
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:SASL_PLAINTEXT
KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL: PLAIN
KAFKA_SASL_ENABLED_MECHANISMS: PLAIN
KAFKA_AUTHORIZER_CLASS_NAME: org.apache.kafka.metadata.authorizer.StandardAuthorizer
KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND: "false"
KAFKA_SUPER_USERS: "User:admin"
```

**kafka_server_jaas.conf — credential store:**

```
KafkaServer {
  org.apache.kafka.common.security.plain.PlainLoginModule required
  username="admin" password="admin-secret"
  user_admin="admin-secret"
  user_order-producer="order-secret"
  user_notification-consumer="notif-secret";
};
```

---

## Spring Boot SASL Configuration

**Client-side SASL setup for each service:**

```yaml
# order-service/application.yml
spring:
  kafka:
    properties:
      security.protocol: SASL_PLAINTEXT
      sasl.mechanism: PLAIN
      sasl.jaas.config: >
        org.apache.kafka.common.security.plain.PlainLoginModule required
        username="order-producer" password="order-secret";
```

```yaml
# notification-service/application.yml
spring:
  kafka:
    properties:
      security.protocol: SASL_PLAINTEXT
      sasl.mechanism: PLAIN
      sasl.jaas.config: >
        org.apache.kafka.common.security.plain.PlainLoginModule required
        username="notification-consumer" password="notif-secret";
```

---

## acl-init — Automated ACL Setup

**One-shot container creates topics and ACLs at startup:**

```bash
# acl-init.sh runs via docker compose:

# Create topic (as admin)
kafka-topics --bootstrap-server kafka:9092 \
  --command-config /etc/kafka/secrets/admin.conf \
  --create --topic 18.orders.created --partitions 3

# Grant WRITE to order-producer
kafka-acls --add --allow-principal User:order-producer \
  --operation Write --operation Describe \
  --topic 18.orders.created ...

# Grant READ to notification-consumer + group access
kafka-acls --add --allow-principal User:notification-consumer \
  --operation Read --operation Describe \
  --topic 18.orders.created ...
kafka-acls --add --allow-principal User:notification-consumer \
  --operation Read --group notification-service-group ...
```

> Treat ACL setup as infrastructure-as-code — never configure manually in production.

---

## TopicAuthorizationException — Violation Demo

**What happens when a client tries an unauthorized operation:**

```bash
# Demo: try to READ from order-producer (which only has WRITE)
curl http://localhost:8082/api/notifications/simulate-violation

# order-producer client attempts: consumer.poll() on 18.orders.created
# → Kafka checks ACL: User:order-producer, READ, 18.orders.created
# → ACL not found, ALLOW_EVERYONE_IF_NO_ACL_FOUND=false
# → TopicAuthorizationException: Not authorized to access topics: [18.orders.created]
```

<br>

**Log output:**
```
ERROR o.a.k.c.consumer.internals.Fetcher: FETCH failed due to
  org.apache.kafka.common.errors.TopicAuthorizationException:
  Not authorized to access topics: [18.orders.created]
```

> Authorization failures are logged with full principal and operation context.

---

## SASL_PLAINTEXT vs SASL_SSL

**Choosing the right security protocol:**

```
SASL_PLAINTEXT (branch18):
  Authentication: ✓ (SASL/PLAIN — password verified)
  Encryption:     ✗ (traffic in plain text on the wire)
  Risk: password visible to network sniffers
  Use case: internal datacenter network with firewall

SASL_SSL (branch23):
  Authentication: ✓ (SASL/PLAIN)
  Encryption:     ✓ (TLS — all traffic encrypted)
  Risk: none for typical threat models
  Use case: production with public/shared networks

SSL only:
  Authentication: optional (mutual TLS / mTLS)
  Encryption:     ✓
  Use case: encryption without username/password auth
```

> For any public-facing or cloud Kafka: always **SASL_SSL**.

---

## Key Takeaways

- **SASL/PLAIN** = username/password authentication per Kafka client
- **ACL** = declarative rules: which principal can perform which operation on which resource
- **`ALLOW_EVERYONE_IF_NO_ACL_FOUND=false`** = deny by default — the safest posture
- **Super Users** bypass all ACL checks — use sparingly (admin operations only)
- **`acl-init`** container = infrastructure-as-code for topic + ACL provisioning
- **`TopicAuthorizationException`** = clear signal that ACL is missing or wrong
- **SASL_PLAINTEXT** for internal networks; **SASL_SSL** for production/cloud

<br>

> Least privilege: each service gets exactly the permissions it needs — no more.

---

## What's Next — Branch 19

**Production-like Demo — all patterns combined**

- Saga choreography (branch13) + idempotent producer (branch12)
- Manual commit (branch06) + DLT (branch09)
- SASL/PLAIN + ACL (branch18) + 3-broker cluster (branch16)
- Avro + Schema Registry (branch11) + Prometheus + Grafana (branch17)
- 5 SASL users, 9 topics with RF=3, full observability

---

<!-- _class: title -->

## What's Next — Branch 19
- Production-ready: all patterns in one stack
- 3 brokers + SASL + ACL + Schema Registry
- Saga + idempotent + manual commit + DLT
- Full Grafana observability

# Questions?

**Branch 18 — Security: SASL/PLAIN + ACL**