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

# Branch 23
# Security: SSL/TLS (SASL\_SSL)

**Apache Kafka for Certification & Production**

---

## Agenda

1. Why TLS encryption is needed beyond SASL
2. Security protocols comparison
3. SSL certificate chain — CA → broker → client
4. Keystore vs Truststore
5. `generate-certs.sh` — automated certificate generation
6. Kafka broker SSL configuration
7. Spring Boot SASL\_SSL client configuration
8. One-way SSL vs mutual TLS (mTLS)
9. Key Takeaways & What's Next

---

## Why TLS Encryption Is Needed

**SASL_PLAINTEXT (branch18): credentials verified but traffic exposed:**

```
SASL_PLAINTEXT problem:
  Client sends: username=order-producer, password=order-secret
  Kafka verifies: ✓ correct credentials
  But on the wire: all data in PLAIN TEXT

  Network sniffer sees:
    username=order-producer
    password=order-secret
    {"orderId":"abc","amount":149.99,"userId":"alice"}
    ← entire message payload visible!

SASL_SSL solution:
  All traffic encrypted with TLS
  Network sniffer sees: binary encrypted gibberish
  Authentication still via SASL/PLAIN (but credentials also encrypted)
```

---

## Security Protocols Comparison

**Four options — choose based on environment:**

| Protocol | Authentication | Encryption | Use Case |
|----------|---------------|-----------|---------|
| `PLAINTEXT` | None | None | Local dev only |
| `SASL_PLAINTEXT` | SASL/PLAIN | None | Internal network with firewall |
| `SSL` | Optional (mTLS) | TLS | Encryption without username auth |
| `SASL_SSL` | SASL/PLAIN | TLS | **Production — both auth + encryption** |

<br>

> For any cloud Kafka, public-facing broker, or multi-tenant environment:
> **`SASL_SSL` is the only acceptable choice.**

---

## SSL Certificate Chain

**Trust is established through a chain of signatures:**

```
Certificate Authority (CA):
  CA generates: ca-key (private) + ca-cert (public)
  CA signs broker certificates — establishes trust

Broker Certificate:
  kafka-broker generates: broker private key + CSR
  CA signs CSR → broker certificate
  Stored in: kafka.server.keystore.jks

Client Trust:
  kafka.client.truststore.jks contains: ca-cert
  Client sees broker cert → signed by trusted CA → trust established ✓

                 CA (ca-key + ca-cert)
                        │ signs
                        ▼
              Broker Certificate
           (in kafka.server.keystore.jks)
                        │ verified against
                        ▼
              CA cert in client truststore
```

---

## Keystore vs Truststore

**Two JKS files with different purposes:**

```
Keystore (MY identity):            Truststore (WHO I TRUST):
  Contains:                          Contains:
    Private key                        CA certificate(s)
    My certificate                     (public certs only)

  kafka.server.keystore.jks          kafka.server.truststore.jks
  → broker's private key              → broker trusts this CA
  → broker's signed certificate       kafka.client.truststore.jks
                                      → clients trust broker certs
                                        signed by this CA

Analogy:
  Keystore = your passport          Truststore = list of trusted issuers
  "This is who I am"                "I accept IDs from these authorities"
```

---

## generate-certs.sh — Certificate Generation

**Full certificate chain in one script:**

```bash
# Step 1: Generate CA (self-signed)
openssl req -new -x509 -keyout ca-key -out ca-cert -days 365 \
  -subj "/CN=KafkaLab-CA/OU=KafkaLab/O=KafkaLab/C=UA"

# Step 2: Broker keystore — private key + CSR
keytool -keystore kafka.server.keystore.jks -alias broker \
  -genkey -keyalg RSA -dname "CN=kafka,OU=KafkaLab,O=KafkaLab,C=UA"
keytool -keystore kafka.server.keystore.jks -alias broker \
  -certreq -file cert-req

# Step 3: CA signs the broker CSR
openssl x509 -req -CA ca-cert -CAkey ca-key \
  -in cert-req -out cert-signed -days 365 -CAcreateserial

# Step 4: Import CA + signed cert into keystore
keytool -keystore kafka.server.keystore.jks -alias CARoot -import -file ca-cert
keytool -keystore kafka.server.keystore.jks -alias broker -import -file cert-signed

# Step 5: Client truststore — only needs CA cert
keytool -keystore kafka.client.truststore.jks -alias CARoot -import -file ca-cert
```

---

## Kafka Broker SSL Configuration

**docker-compose-23.yml environment variables:**

```yaml
KAFKA_LISTENERS: CONTROLLER://:9093,SASL_SSL://:9092,SASL_SSL_HOST://:9128
KAFKA_INTER_BROKER_LISTENER_NAME: SASL_SSL
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: >
  CONTROLLER:PLAINTEXT,SASL_SSL:SASL_SSL,SASL_SSL_HOST:SASL_SSL
KAFKA_SSL_KEYSTORE_FILENAME: certs/kafka.server.keystore.jks
KAFKA_SSL_KEYSTORE_CREDENTIALS: ssl_keystore_password
KAFKA_SSL_KEYSTORE_PASSWORD: kafkalab-ssl-secret
KAFKA_SSL_KEY_CREDENTIALS: ssl_keystore_password
KAFKA_SSL_TRUSTSTORE_FILENAME: certs/kafka.server.truststore.jks
KAFKA_SSL_TRUSTSTORE_CREDENTIALS: ssl_truststore_password
KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM: ""  # disable hostname verification (dev)
```

> In production: remove `ssl.endpoint.identification.algorithm=""` — enable hostname verification.

---

## Spring Boot SASL\_SSL Client Configuration

**Client needs: SASL credentials + SSL truststore:**

```yaml
# order-service/application.yml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    properties:
      security.protocol: SASL_SSL           # ← both auth + encryption
      sasl.mechanism: PLAIN
      sasl.jaas.config: >
        org.apache.kafka.common.security.plain.PlainLoginModule required
        username="order-producer" password="order-secret";
      ssl.truststore.location: /etc/kafka/secrets/certs/kafka.client.truststore.jks
      ssl.truststore.password: kafkalab-ssl-secret
      ssl.endpoint.identification.algorithm: ""   # dev only
```

> In production: `ssl.endpoint.identification.algorithm: https` to verify broker hostname.

---

## Key Takeaways

- **SASL_SSL** = SASL/PLAIN authentication + TLS encryption — production standard
- **Certificate chain**: CA signs broker cert → client trusts CA → client trusts broker
- **Keystore** = your identity (private key + certificate); **Truststore** = trusted CAs
- **`generate-certs.sh`** = complete PKI chain in one script — treat as infrastructure code
- **One-way SSL** = client verifies broker identity (most common)
- **mTLS** = broker also verifies client certificate (highest security)
- **`ssl.endpoint.identification.algorithm=""`** = disable hostname check (dev ONLY)
- In cloud Kafka (Confluent Cloud, MSK): SSL certs managed automatically

<br>

> SASL_PLAINTEXT for dev, SASL_SSL for everything else. No exceptions in production.

---

## What's Next — Branch 24

**Advanced Topics: Compression, AdminClient, Interceptors**

- Compression: `gzip`, `lz4`, `zstd`, `snappy` — size vs CPU trade-off
- `AdminClient` API — programmatic topic management without CLI
- `ProducerInterceptor` — `onSend()`, `onAcknowledgement()` hooks
- `ConsumerInterceptor` — `onConsume()`, `onCommit()` hooks
- Benchmark: compare all compression types in code

---

<!-- _class: title -->

## What's Next — Branch 24
- Compression: lz4 and zstd for production
- AdminClient: runtime topic management
- Interceptors: metrics, tracing, header enrichment
- Quotas: rate-limit per client

# Questions?

**Branch 23 — Security: SSL/TLS (SASL\_SSL)**