# Branch 23 — Security: SSL/TLS (SASL_SSL)

## Що вивчаємо

| Концепція | Деталі |
|-----------|--------|
| **SSL/TLS** | Шифрування трафіку між клієнтами та брокером |
| **SASL_SSL** | SASL аутентифікація + SSL шифрування (поєднання branch18 + SSL) |
| **Keystore / Truststore** | keystore = приватний ключ + сертифікат; truststore = довірені CA сертифікати |
| **One-way SSL** | Клієнт перевіряє сертифікат брокера (але не навпаки) |
| **Certificate chain** | CA → broker cert → підписаний і зареєстрований в keystore |

## SSL vs SASL — різниця

```
SASL (branch18):
  Аутентифікація: хто ти?
  Транспорт: PLAINTEXT (незашифрований!)
  Протокол: SASL_PLAINTEXT

SSL:
  Шифрування: ніхто не може перехопити трафік
  Аутентифікація: можлива через клієнтські сертифікати (mTLS)
  Протокол: SSL

SASL_SSL (branch23):
  Аутентифікація: via SASL/PLAIN (логін/пароль)
  Шифрування: via SSL/TLS
  Протокол: SASL_SSL ← best of both worlds
```

## Архітектура

```
order-service (user: order-producer)
  SASL_SSL ──encrypted + authenticated──► Kafka (SASL_SSL listener, port 9128)
                                               │
notification-service (user: notification-consumer)
  SASL_SSL ──encrypted + authenticated──►     │
```

## Порти

| Сервіс | Порт |
|--------|------|
| Kafka (external, SASL_SSL) | 9128 |
| Kafka UI | 8206 |
| order-service | 8207 |
| notification-service | 8208 |

---

## Підготовка: генерація сертифікатів

**Обов'язково** виконати до `docker compose up`:

```bash
cd branch23_ssl/kafka
bash generate-certs.sh
```

Скрипт створює в `branch23_ssl/kafka/certs/`:
- `ca-key`, `ca-cert` — Certificate Authority (CA)
- `kafka.server.keystore.jks` — брокер: приватний ключ + сертифікат підписаний CA
- `kafka.server.truststore.jks` — брокер: довіряє нашому CA
- `kafka.client.truststore.jks` — клієнти: довіряють сертифікату брокера (через CA)

```
CA (Certificate Authority)
  └── підписує ──► Kafka broker certificate
                   (зберігається в kafka.server.keystore.jks)

Клієнт хоче довіряти брокеру:
  kafka.client.truststore.jks містить CA cert
  → клієнт перевіряє: "broker cert підписаний цим CA? ТАК → довіряю"
```

## Запуск

```bash
# Крок 1: Згенерувати сертифікати
cd branch23_ssl/kafka && bash generate-certs.sh && cd ../..

# Крок 2: Запустити стек
docker compose -f docker-compose-23.yml up --build
```

---

## Як протестувати

### 1. Нормальна відправка (SASL_SSL)

```bash
curl -X POST "http://localhost:8207/api/orders?userId=user-1&totalAmount=199.9&category=ELECTRONICS"
# Очікуємо: 200 OK, лог: [SASL_SSL] Sent orderId=...
```

### 2. Перевірити що notification-service отримав (через шифрований канал)

```bash
curl -s http://localhost:8208/api/notifications/count | jq
# {"received": 1}
```

### 3. Верифікація SSL сертифікату

```bash
# Перевірити що брокер дійсно використовує SSL
openssl s_client -connect localhost:9128 \
  -CAfile branch23_ssl/kafka/certs/ca-cert \
  -verify_return_error 2>&1 | grep -E "Verify|subject|issuer|Protocol"

# Очікуваний результат:
# subject=CN=kafka, OU=KafkaLab, ...
# issuer=CN=KafkaLab-CA, ...
# Protocol: TLSv1.3
# Verify return code: 0 (ok)
```

### 4. Спроба підключення без SSL → відхиляється

```bash
# Спробуємо PLAINTEXT — Kafka відхилить
kafka-topics --bootstrap-server localhost:9128 --list
# Error: Connection to node -1 (localhost/127.0.0.1:9128) failed authentication
```

### 5. ACL violation (як у branch18)

```bash
# notification-consumer намагається писати — заборонено ACL
curl -X POST http://localhost:8208/api/probe/write-attempt | jq
# {"result": "DENIED", "exception": "TopicAuthorizationException"}
```

---

## Spring Boot SASL_SSL конфігурація

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
      ssl.endpoint.identification.algorithm: ""  # вимкнути hostname verification для localhost
```

## Параметри SSL

| Параметр | Значення | Пояснення |
|----------|----------|-----------|
| `security.protocol` | `SASL_SSL` | SASL аутентифікація + SSL шифрування |
| `ssl.truststore.location` | шлях до .jks | Де знаходиться truststore з CA cert |
| `ssl.truststore.password` | пароль | Пароль для truststore |
| `ssl.keystore.location` | (для mTLS) | Клієнтський keystore (якщо потрібна двостороння аутентифікація) |
| `ssl.endpoint.identification.algorithm` | `""` | Вимкнути перевірку hostname (потрібно для localhost) |

---

## Ключові концепції

| Концепція | Що демонструє |
|-----------|---------------|
| **SASL_SSL** | SASL (хто?) + SSL (шифрування) = best practice для production |
| **One-way SSL** | Клієнт перевіряє broker cert; broker не перевіряє client cert |
| **CA (Certificate Authority)** | Підписує сертифікати — основа довіри |
| **JKS (Java KeyStore)** | Формат зберігання сертифікатів/ключів для Java |
| **`ssl.endpoint.identification.algorithm`** | `""` = вимкнути hostname verification (dev only!) |
| **SASL_PLAINTEXT vs SASL_SSL** | Різниця лише у шифруванні транспорту |