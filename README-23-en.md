# Branch 23 — Security: SSL/TLS (SASL_SSL)

## 1. What we learn in this branch

- **SSL/TLS** — encryption of traffic between clients and the Kafka broker.
- **SASL_SSL** — combining SASL authentication (branch18) with SSL encryption.
- **Keystore / Truststore** — keystore holds the private key + certificate;
  truststore holds trusted CA certificates.
- **One-way SSL** — the client verifies the broker's certificate (but not vice versa).
- **Certificate Authority (CA)** — a certification authority that signs broker certificates.
- **`generate-certs.sh`** — generates the full chain: CA → broker cert → JKS keystores.

---

## 2. Changes compared to the previous branch (branch22)

- **Kafka listener** changed: `SASL_SSL` instead of `SASL_PLAINTEXT` or `PLAINTEXT`.
- **Added** `generate-certs.sh` — script for generating SSL certificates.
- **Added** `kafka.server.keystore.jks`, `kafka.server.truststore.jks`,
  `kafka.client.truststore.jks`.
- **Spring Boot config** extended: `ssl.truststore.location`, `ssl.truststore.password`.
- **Listener port** `9128` for SASL_SSL (external).
- **Mandatory** step before startup: `bash generate-certs.sh`.

---

## 3. Architecture

```
order-service (user: order-producer)
  SASL_SSL ── encrypted + authenticated ──► Kafka (SASL_SSL, port 9128)
                                                   │
notification-service (user: notification-consumer) │
  SASL_SSL ── encrypted + authenticated ──────────◄┘
```

**SSL chain of trust:**
```
Certificate Authority (CA)
  └── signs ──► Kafka broker certificate
               (stored in kafka.server.keystore.jks)

Client verification:
  kafka.client.truststore.jks contains the CA cert
  → client sees broker cert → signed by our CA → trusted ✓
```

**Services and ports (from docker-compose-23.yml):**
- `kafka` — 9092 (external, SASL_SSL listener mapped from internal 9128)
- `kafka-ui` — 8080 → http://localhost:8080
- `acl-init-b23` — one-shot (no port)
- `order-service-b23` — 8081 → http://localhost:8081
- `notification-service-b23` — 8082 → http://localhost:8082

---

## 4. Key Concepts

### SASL_PLAINTEXT vs SASL_SSL vs SSL

```
SASL_PLAINTEXT (branch18):
  Authentication: ✓ (SASL/PLAIN username/password)
  Encryption:     ✗ (traffic in plaintext!)
  Protocol: SASL_PLAINTEXT
  Use case: internal network behind a firewall

SSL:
  Authentication: optional (mutual TLS / mTLS)
  Encryption:     ✓ (TLS)
  Protocol: SSL
  Use case: encryption without SASL

SASL_SSL (branch23):
  Authentication: ✓ (SASL/PLAIN)
  Encryption:     ✓ (TLS)
  Protocol: SASL_SSL ← best of both worlds
  Use case: production with a public network
```

### Certificate generation

```bash
# branch23_ssl/kafka/generate-certs.sh
# Step 1: Generate CA (Certificate Authority)
openssl req -new -x509 -keyout ca-key -out ca-cert -days 365 \
  -subj "/CN=KafkaLab-CA/OU=KafkaLab/O=KafkaLab/C=UA"

# Step 2: Broker keystore (private key + broker cert signed by CA)
keytool -keystore kafka.server.keystore.jks -alias broker -genkey \
  -keyalg RSA -dname "CN=kafka,OU=KafkaLab,..."
keytool -keystore kafka.server.keystore.jks -alias broker -certreq -file cert-req
openssl x509 -req -CA ca-cert -CAkey ca-key -in cert-req -out cert-signed -days 365
keytool -keystore kafka.server.keystore.jks -alias CARoot -import -file ca-cert
keytool -keystore kafka.server.keystore.jks -alias broker -import -file cert-signed

# Step 3: Broker truststore (trusts our CA)
keytool -keystore kafka.server.truststore.jks -alias CARoot -import -file ca-cert

# Step 4: Client truststore (clients trust broker cert via CA)
keytool -keystore kafka.client.truststore.jks -alias CARoot -import -file ca-cert
```

### Kafka SSL configuration (broker)

```yaml
# docker-compose-23.yml
KAFKA_LISTENERS: CONTROLLER://:9093,SASL_SSL://:9092,SASL_SSL_HOST://:9128
KAFKA_INTER_BROKER_LISTENER_NAME: SASL_SSL
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,SASL_SSL:SASL_SSL,SASL_SSL_HOST:SASL_SSL
KAFKA_SSL_KEYSTORE_FILENAME: certs/kafka.server.keystore.jks
KAFKA_SSL_KEYSTORE_CREDENTIALS: ssl_keystore_password
KAFKA_SSL_TRUSTSTORE_FILENAME: certs/kafka.server.truststore.jks
KAFKA_SSL_TRUSTSTORE_CREDENTIALS: ssl_keystore_password
KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM: ""  # disable hostname verification
```

### Spring Boot SASL_SSL configuration

```yaml
# order-service application.yml
spring:
  kafka:
    properties:
      security.protocol: SASL_SSL
      sasl.mechanism: PLAIN
      sasl.jaas.config: >
        org.apache.kafka.common.security.plain.PlainLoginModule required
        username="order-producer" password="order-secret";
      ssl.truststore.location: /etc/kafka/secrets/certs/kafka.client.truststore.jks
      ssl.truststore.password: kafkalab-ssl-secret
      ssl.endpoint.identification.algorithm: ""  # for localhost (dev only)
```

---

## 5. How to Run

```bash
# Step 1: Generate certificates BEFORE starting — this is mandatory!
cd branch23_ssl/kafka && bash generate-certs.sh && cd ../..

# Step 2: Start the stack
docker compose -f docker-compose-23.yml up --build

# Kafka UI: http://localhost:8080
```

---

## 6. How to Test

### Test 1 — Normal send (SASL_SSL)

```bash
curl -X POST "http://localhost:8081/api/orders?userId=user-1&totalAmount=199.9&category=ELECTRONICS"
# 200 OK
# order-service log: [SASL_SSL] Sent orderId=...
```

### Test 2 — notification-service received

```bash
curl -s http://localhost:8082/api/notifications/count | jq .
# {"received": 1}
```

### Test 3 — SSL certificate verification

```bash
# Confirm the broker is actually using SSL
openssl s_client -connect localhost:9092 \
  -CAfile branch23_ssl/kafka/certs/ca-cert \
  -verify_return_error 2>&1 | grep -E "Verify|subject|issuer|Protocol"

# Expected output:
# subject=CN=kafka, OU=KafkaLab, ...
# issuer=CN=KafkaLab-CA, ...
# Protocol: TLSv1.3
# Verify return code: 0 (ok)
```

### Test 4 — Connect without SSL

```bash
# Attempt PLAINTEXT — Kafka will reject it
kafka-topics --bootstrap-server localhost:9092 --list
# Error: Connection SSL handshake failed or authentication failed
```

### Test 5 — ACL violation

```bash
# notification-consumer attempts to write → denied by ACL
curl -X POST http://localhost:8082/api/probe/write-attempt | jq .
# {"result": "DENIED", "exception": "TopicAuthorizationException"}
```

### Test 6 — ACL list

```bash
docker exec acl-init-b23 kafka-acls \
  --bootstrap-server kafka:9092 \
  --command-config /etc/kafka/secrets/admin.conf \
  --list
```

---

## 7. Deep Dive

### Keystore vs Truststore

```
Keystore (kafka.server.keystore.jks):
  Contains: private key + own certificate
  Used by: broker (proves its identity)
  Analogy: a passport (identity document)

Truststore (kafka.client.truststore.jks):
  Contains: CA certificates (trusted authorities)
  Used by: clients (to verify the broker)
  Analogy: a list of trusted passport issuers

One-way SSL (current configuration):
  Broker has keystore ✓
  Clients have truststore ✓
  Clients do NOT have keystore (broker does not verify clients)

Two-way SSL (mTLS):
  Broker has keystore ✓
  Clients ALSO have keystore ✓ (broker verifies clients via cert)
  No SASL needed (cert = authentication)
```

### TLS Handshake

```
Client                              Kafka Broker
  │── ClientHello (TLS 1.3) ─────►  │
  │◄── ServerHello + Certificate ── │
  │    (kafka.server.keystore cert)  │
  │                                  │
  │  Verify cert against CA:         │
  │  truststore CA → did it sign cert?│
  │  YES → Continue                  │
  │── ClientFinished ──────────────► │
  │◄── ServerFinished ────────────── │
  │        (TLS established)         │
  │── SASL PLAIN credentials ──────► │  (over TLS)
  │◄── SASL OK ─────────────────────  │
```

### `ssl.endpoint.identification.algorithm: ""`

```
By default: client verifies that the hostname in the cert = the server hostname

CN=kafka in the certificate
Connection: localhost:9092

localhost ≠ kafka → SSL handshake failed!

Dev workaround: ssl.endpoint.identification.algorithm=""
In production: CN=actual_hostname or SAN extension
```

---

## 8. Project Structure

```
branch23_ssl/
├── kafka/
│   ├── generate-certs.sh              ← generate CA + keystores + truststores
│   ├── kafka_server_jaas.conf         ← SASL users (same as branch18)
│   ├── admin.conf                     ← admin SASL_SSL client properties
│   ├── acl-init.sh                    ← create topics + ACLs
│   ├── ssl_keystore_password          ← password file
│   └── certs/                         ← (generated by generate-certs.sh)
│       ├── ca-cert, ca-key
│       ├── kafka.server.keystore.jks
│       ├── kafka.server.truststore.jks
│       └── kafka.client.truststore.jks
├── order-service/
│   └── application.yml                ← security.protocol: SASL_SSL + truststore
└── notification-service/
    ├── controller/AclProbeController.kt
    └── application.yml                ← security.protocol: SASL_SSL + truststore
```

---

## 9. What's Next

- **Branch 24** — Advanced Topics: Compression (gzip/lz4/zstd), AdminClient API,
  ProducerInterceptor, ConsumerInterceptor.

---

## 10. Lecture Slides

### Slide 1 — SSL Certificate Chain

```
Certificate Authority (CA)
  ├── CA private key (ca-key)
  └── CA certificate (ca-cert)
           │ signs
           ▼
     Broker Certificate
       ├── broker private key (in keystore)
       └── broker cert signed by CA (in keystore)

Client verification:
  "broker cert signed by CA, which I trust"
  (CA cert is in client truststore)
```

### Slide 2 — Security Protocols Comparison

```
Protocol         Auth    Encryption    Use case
PLAINTEXT        ✗        ✗           Dev only
SASL_PLAINTEXT   ✓        ✗           Internal secure network
SSL              ✗/✓      ✓           Encryption only
SASL_SSL         ✓        ✓           Production (recommended)
```

### Slide 3 — Keystore vs Truststore

```
Keystore:             Truststore:
  My Identity           Who I Trust
  
  [Private Key]         [CA Certificate]
  [My Certificate]      [CA Certificate 2]
  
  "I am kafka broker"   "I trust certs from these CAs"
```

---

## 11. Demo Script

```bash
#!/bin/bash
echo "=== Branch 23: SSL/TLS Demo ==="

echo "=== Step 1: Generate certificates ==="
cd branch23_ssl/kafka && bash generate-certs.sh && cd ../..

echo ""
echo "=== Step 2: Start ==="
docker compose -f docker-compose-23.yml up --build -d
sleep 60

echo ""
echo "=== Test 1: Normal send ==="
curl -s -X POST "http://localhost:8081/api/orders?userId=user-1&totalAmount=299.9&category=ELECTRONICS" | jq .

echo ""
echo "=== Test 2: Notification received ==="
curl -s http://localhost:8082/api/notifications/count | jq .

echo ""
echo "=== Test 3: SSL verification ==="
openssl s_client -connect localhost:9092 \
  -CAfile branch23_ssl/kafka/certs/ca-cert \
  -verify_return_error 2>&1 | grep -E "Verify|subject|Protocol"

echo ""
echo "=== Test 4: ACL violation ==="
curl -s -X POST http://localhost:8082/api/probe/write-attempt | jq .

echo ""
echo "=== ACL list ==="
docker exec acl-init-b23 kafka-acls \
  --bootstrap-server kafka:9092 \
  --command-config /etc/kafka/secrets/admin.conf \
  --list --topic 23.orders.created
```

---

## 12. Self-Check Questions

- What is the difference between SASL_PLAINTEXT and SASL_SSL?
- What is stored in the keystore and what is stored in the truststore?
- What is a CA (Certificate Authority) and why is it needed?
- What is one-way SSL and how does it differ from mTLS?
- Why is `ssl.endpoint.identification.algorithm=""`? Can it be left as the default?
- Why must `generate-certs.sh` be run BEFORE `docker compose up`?
- How does a TLS handshake differ from a SASL handshake?
- What would change if mTLS were used instead of SASL_SSL?