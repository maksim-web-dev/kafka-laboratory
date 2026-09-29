# Branch 23 — Security: SSL/TLS (SASL_SSL)

## 1. Що вивчаємо у цій гілці

- **SSL/TLS** — шифрування трафіку між клієнтами та Kafka broker.
- **SASL_SSL** — поєднання SASL аутентифікації (branch18) зі SSL шифруванням.
- **Keystore / Truststore** — keystore містить приватний ключ + сертифікат;
  truststore містить довірені CA сертифікати.
- **One-way SSL** — клієнт перевіряє сертифікат брокера (але не навпаки).
- **Certificate Authority (CA)** — центр сертифікації, підписує сертифікати брокера.
- **`generate-certs.sh`** — генерація всього ланцюга: CA → broker cert → JKS keystores.

---

## 2. Зміни порівняно з попередньою гілкою (branch22)

- **Kafka listener** змінено: `SASL_SSL` замість `SASL_PLAINTEXT` або `PLAINTEXT`.
- **Додано** `generate-certs.sh` — скрипт для генерації SSL сертифікатів.
- **Додано** `kafka.server.keystore.jks`, `kafka.server.truststore.jks`,
  `kafka.client.truststore.jks`.
- **Spring Boot config** розширено: `ssl.truststore.location`, `ssl.truststore.password`.
- **Listener порт** `9128` для SASL_SSL (external).
- **Обов'язковий** крок перед запуском: `bash generate-certs.sh`.

---

## 3. Архітектура

```
order-service (user: order-producer)
  SASL_SSL ── шифрований + автентифікований ──► Kafka (SASL_SSL, port 9128)
                                                       │
notification-service (user: notification-consumer)     │
  SASL_SSL ── шифрований + автентифікований ──────────◄┘
```

**SSL ланцюг довіри:**
```
Certificate Authority (CA)
  └── підписує ──► Kafka broker certificate
                   (в kafka.server.keystore.jks)

Клієнт перевіряє:
  kafka.client.truststore.jks містить CA cert
  → клієнт бачить broker cert → він підписаний нашим CA → довіряю ✓
```

**Сервіси та порти (з docker-compose-23.yml):**
- `kafka` — 9092 (external, SASL_SSL listener mapped from internal 9128)
- `kafka-ui` — 8080 → http://localhost:8080
- `acl-init-b23` — одноразовий (без port)
- `order-service-b23` — 8081 → http://localhost:8081
- `notification-service-b23` — 8082 → http://localhost:8082

---

## 4. Ключові концепції

### SASL_PLAINTEXT vs SASL_SSL vs SSL

```
SASL_PLAINTEXT (branch18):
  Аутентифікація: ✓ (SASL/PLAIN логін/пароль)
  Шифрування:     ✗ (трафік у відкритому вигляді!)
  Протокол: SASL_PLAINTEXT
  Use case: внутрішня мережа з firewall

SSL:
  Аутентифікація: опціонально (mutual TLS / mTLS)
  Шифрування:     ✓ (TLS)
  Протокол: SSL
  Use case: шифрування без SASL

SASL_SSL (branch23):
  Аутентифікація: ✓ (SASL/PLAIN)
  Шифрування:     ✓ (TLS)
  Протокол: SASL_SSL ← best of both worlds
  Use case: production з публічною мережею
```

### Генерація сертифікатів

```bash
# branch23_ssl/kafka/generate-certs.sh
# Крок 1: Генерати CA (Certificate Authority)
openssl req -new -x509 -keyout ca-key -out ca-cert -days 365 \
  -subj "/CN=KafkaLab-CA/OU=KafkaLab/O=KafkaLab/C=UA"

# Крок 2: Broker keystore (приватний ключ + broker cert підписаний CA)
keytool -keystore kafka.server.keystore.jks -alias broker -genkey \
  -keyalg RSA -dname "CN=kafka,OU=KafkaLab,..."
keytool -keystore kafka.server.keystore.jks -alias broker -certreq -file cert-req
openssl x509 -req -CA ca-cert -CAkey ca-key -in cert-req -out cert-signed -days 365
keytool -keystore kafka.server.keystore.jks -alias CARoot -import -file ca-cert
keytool -keystore kafka.server.keystore.jks -alias broker -import -file cert-signed

# Крок 3: Broker truststore (довіряє нашому CA)
keytool -keystore kafka.server.truststore.jks -alias CARoot -import -file ca-cert

# Крок 4: Client truststore (клієнти довіряють broker cert через CA)
keytool -keystore kafka.client.truststore.jks -alias CARoot -import -file ca-cert
```

### Kafka SSL конфігурація (broker)

```yaml
# docker-compose-23.yml
KAFKA_LISTENERS: CONTROLLER://:9093,SASL_SSL://:9092,SASL_SSL_HOST://:9128
KAFKA_INTER_BROKER_LISTENER_NAME: SASL_SSL
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,SASL_SSL:SASL_SSL,SASL_SSL_HOST:SASL_SSL
KAFKA_SSL_KEYSTORE_FILENAME: certs/kafka.server.keystore.jks
KAFKA_SSL_KEYSTORE_CREDENTIALS: ssl_keystore_password
KAFKA_SSL_TRUSTSTORE_FILENAME: certs/kafka.server.truststore.jks
KAFKA_SSL_TRUSTSTORE_CREDENTIALS: ssl_keystore_password
KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM: ""  # вимкнути hostname verification
```

### Spring Boot SASL_SSL конфігурація

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
      ssl.endpoint.identification.algorithm: ""  # для localhost (dev only)
```

---

## 5. Як запустити

```bash
# Крок 1: Обов'язково генерувати сертифікати перед запуском!
cd branch23_ssl/kafka && bash generate-certs.sh && cd ../..

# Крок 2: Запустити стек
docker compose -f docker-compose-23.yml up --build

# Kafka UI: http://localhost:8080
```

---

## 6. Як тестувати

### Тест 1 — Нормальна відправка (SASL_SSL)

```bash
curl -X POST "http://localhost:8081/api/orders?userId=user-1&totalAmount=199.9&category=ELECTRONICS"
# 200 OK
# Лог order-service: [SASL_SSL] Sent orderId=...
```

### Тест 2 — Notification-service отримав

```bash
curl -s http://localhost:8082/api/notifications/count | jq .
# {"received": 1}
```

### Тест 3 — Верифікація SSL сертифікату

```bash
# Переконатись що брокер дійсно використовує SSL
openssl s_client -connect localhost:9092 \
  -CAfile branch23_ssl/kafka/certs/ca-cert \
  -verify_return_error 2>&1 | grep -E "Verify|subject|issuer|Protocol"

# Очікуваний результат:
# subject=CN=kafka, OU=KafkaLab, ...
# issuer=CN=KafkaLab-CA, ...
# Protocol: TLSv1.3
# Verify return code: 0 (ok)
```

### Тест 4 — Підключення без SSL

```bash
# Спроба PLAINTEXT — Kafka відхилить
kafka-topics --bootstrap-server localhost:9092 --list
# Помилка: Connection SSL handshake failed або authentication failed
```

### Тест 5 — ACL violation

```bash
# notification-consumer намагається писати → заборонено ACL
curl -X POST http://localhost:8082/api/probe/write-attempt | jq .
# {"result": "DENIED", "exception": "TopicAuthorizationException"}
```

### Тест 6 — ACL список

```bash
docker exec acl-init-b23 kafka-acls \
  --bootstrap-server kafka:9092 \
  --command-config /etc/kafka/secrets/admin.conf \
  --list
```

---

## 7. Поглиблений розгляд

### Keystore vs Truststore

```
Keystore (kafka.server.keystore.jks):
  Містить: приватний ключ + власний сертифікат
  Хто використовує: broker (доводить свою ідентичність)
  Аналогія: паспорт (identity document)

Truststore (kafka.client.truststore.jks):
  Містить: CA сертифікати (довірені authority)
  Хто використовує: клієнти (перевіряють broker)
  Аналогія: список довірених емітентів паспортів

One-way SSL (поточна конфігурація):
  Broker має keystore ✓
  Клієнти мають truststore ✓
  Клієнти НЕ мають keystore (broker не перевіряє клієнтів)

Two-way SSL (mTLS):
  Broker має keystore ✓
  Клієнти ТАКОЖ мають keystore ✓ (broker перевіряє клієнтів через cert)
  Не потрібен SASL (cert = аутентифікація)
```

### TLS Handshake

```
Client                              Kafka Broker
  │── ClientHello (TLS 1.3) ─────►  │
  │◄── ServerHello + Certificate ── │
  │    (kafka.server.keystore cert)  │
  │                                  │
  │  Перевіряємо cert через CA:      │
  │  truststore CA → підписало cert? │
  │  YES → Continue                  │
  │── ClientFinished ──────────────► │
  │◄── ServerFinished ────────────── │
  │        (TLS established)         │
  │── SASL PLAIN credentials ──────► │  (поверх TLS)
  │◄── SASL OK ─────────────────────  │
```

### `ssl.endpoint.identification.algorithm: ""`

```
За замовч: клієнт перевіряє що hostname у cert = hostname сервера

CN=kafka в сертифікаті
Підключення: localhost:9092

localhost ≠ kafka → SSL handshake failed!

Рішення для dev: ssl.endpoint.identification.algorithm=""
В production: CN=actual_hostname або SAN extension
```

---

## 8. Структура проекту

```
branch23_ssl/
├── kafka/
│   ├── generate-certs.sh              ← генерація CA + keystores + truststores
│   ├── kafka_server_jaas.conf         ← SASL users (як у branch18)
│   ├── admin.conf                     ← admin SASL_SSL client properties
│   ├── acl-init.sh                    ← створення топіків + ACL
│   ├── ssl_keystore_password          ← файл з паролем
│   └── certs/                         ← (генерується generate-certs.sh)
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

## 9. Що далі

- **Branch 24** — Advanced Topics: Compression (gzip/lz4/zstd), AdminClient API,
  ProducerInterceptor, ConsumerInterceptor.

---

## 10. Слайди для лекції

### Слайд 1 — SSL Certificate Chain

```
Certificate Authority (CA)
  ├── CA private key (ca-key)
  └── CA certificate (ca-cert)
           │ signs
           ▼
     Broker Certificate
       ├── broker private key (в keystore)
       └── broker cert signed by CA (в keystore)

Клієнт перевіряє:
  "broker cert підписаний CA, якому я довіряю"
  (CA cert є у client truststore)
```

### Слайд 2 — Security Protocols Comparison

```
Protocol         Auth    Encryption    Use case
PLAINTEXT        ✗        ✗           Dev only
SASL_PLAINTEXT   ✓        ✗           Internal secure network
SSL              ✗/✓      ✓           Encryption only
SASL_SSL         ✓        ✓           Production (recommended)
```

### Слайд 3 — Keystore vs Truststore

```
Keystore:             Truststore:
  My Identity           Who I Trust
  
  [Private Key]         [CA Certificate]
  [My Certificate]      [CA Certificate 2]
  
  "Я — kafka broker"    "Я довіряю certs від цих CA"
```

---

## 11. Демонстраційний сценарій

```bash
#!/bin/bash
echo "=== Branch 23: SSL/TLS Demo ==="

echo "=== Крок 1: Генерація сертифікатів ==="
cd branch23_ssl/kafka && bash generate-certs.sh && cd ../..

echo ""
echo "=== Крок 2: Запуск ==="
docker compose -f docker-compose-23.yml up --build -d
sleep 60

echo ""
echo "=== Тест 1: Нормальна відправка ==="
curl -s -X POST "http://localhost:8081/api/orders?userId=user-1&totalAmount=299.9&category=ELECTRONICS" | jq .

echo ""
echo "=== Тест 2: Notification отримав ==="
curl -s http://localhost:8082/api/notifications/count | jq .

echo ""
echo "=== Тест 3: Верифікація SSL ==="
openssl s_client -connect localhost:9092 \
  -CAfile branch23_ssl/kafka/certs/ca-cert \
  -verify_return_error 2>&1 | grep -E "Verify|subject|Protocol"

echo ""
echo "=== Тест 4: ACL violation ==="
curl -s -X POST http://localhost:8082/api/probe/write-attempt | jq .

echo ""
echo "=== ACL список ==="
docker exec acl-init-b23 kafka-acls \
  --bootstrap-server kafka:9092 \
  --command-config /etc/kafka/secrets/admin.conf \
  --list --topic 23.orders.created
```

---

## 12. Питання для самоперевірки

- Яка різниця між SASL_PLAINTEXT та SASL_SSL?
- Що зберігається у keystore і що у truststore?
- Що таке CA (Certificate Authority) і навіщо він потрібен?
- Що таке one-way SSL і чим він відрізняється від mTLS?
- Навіщо `ssl.endpoint.identification.algorithm=""`? Чи можна залишити за замовч?
- Чому `generate-certs.sh` потрібно виконати ДО `docker compose up`?
- Як TLS handshake відрізняється від SASL handshake?
- Що зміниться якщо використовувати mTLS замість SASL_SSL?