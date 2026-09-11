# Branch 18 — Security: SASL/PLAIN + ACL

## Що демонструє ця гілка

Захист Kafka за допомогою **SASL/PLAIN автентифікації** та **ACL (Access Control Lists)**:
- Кожен сервіс має власний логін/пароль
- ACL визначають, хто може читати/писати який топік
- Спроба несанкціонованого запису → `TopicAuthorizationException`

## Архітектура

```
order-service (user: order-producer)   ──WRITE──►  Kafka (SASL_PLAINTEXT + ACL)
                                                          │
notification-service (user: notification-consumer) ──READ──►  18.orders.created
```

| Компонент      | Порт  | Роль                                         |
|----------------|-------|----------------------------------------------|
| Kafka (KRaft)  | 9121  | Брокер з SASL/PLAIN + ACL enforcement        |
| acl-init       | —     | Одноразовий контейнер: створює топік та ACL  |
| kafka-ui       | 8186  | Веб-інтерфейс (admin credentials)            |
| order-service  | 8187  | Продюсер (user: order-producer)              |
| notification-service | 8188 | Консьюмер (user: notification-consumer) |

## SASL користувачі

| Користувач            | Пароль        | Роль                   |
|-----------------------|---------------|------------------------|
| `admin`               | `admin-secret`| Суперюзер, обходить ACL|
| `order-producer`      | `order-secret`| WRITE до orders топіку |
| `notification-consumer`| `notif-secret`| READ з orders топіку  |

## ACL правила

| Principal               | Ресурс              | Операція           |
|-------------------------|---------------------|--------------------|
| `User:order-producer`   | topic `18.orders.created` | Write, Describe |
| `User:notification-consumer` | topic `18.orders.created` | Read, Describe |
| `User:notification-consumer` | group `notification-service-group` | Read |

## Запуск

```bash
docker compose -f docker-compose-18.yml up --build
```

Kafka UI: http://localhost:8186

## Демонстрація

### 1. Нормальна відправка (order-producer → WRITE ACL є)

```bash
curl -X POST "http://localhost:8187/api/orders?userId=user-1&totalAmount=199.9&category=ELECTRONICS"
```

Очікуваний результат: `200 OK` з orderId. Логи order-service покажуть `[SEC] Sent orderId=...`

### 2. Демонстрація ACL violation (notification-consumer → WRITE заборонено)

```bash
curl -X POST http://localhost:8188/api/probe/write-attempt
```

Очікуваний результат:
```json
{
  "result": "DENIED",
  "exception": "TopicAuthorizationException",
  "message": "Not authorized to access topics: [18.orders.created]",
  "explanation": "notification-consumer has READ ACL only — WRITE is forbidden by Kafka ACL"
}
```

### 3. Перегляд ACL через CLI

```bash
docker exec acl-init-b18 kafka-acls \
  --bootstrap-server kafka:9092 \
  --command-config /etc/kafka/secrets/admin.conf \
  --list
```

### 4. Спроба підключення без пароля

```bash
# Без SASL — відхиляється на рівні протоколу
kafka-topics --bootstrap-server localhost:9121 --list
# SASL authentication failed
```

## Ключові Kafka налаштування

```yaml
# docker-compose-18.yml — kafka service
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:SASL_PLAINTEXT,PLAINTEXT_HOST:SASL_PLAINTEXT
KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL: PLAIN
KAFKA_SASL_ENABLED_MECHANISMS: PLAIN
KAFKA_AUTHORIZER_CLASS_NAME: org.apache.kafka.metadata.authorizer.StandardAuthorizer
KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND: false   # deny by default
KAFKA_SUPER_USERS: User:admin
KAFKA_OPTS: -Djava.security.auth.login.config=/etc/kafka/secrets/kafka_server_jaas.conf
```

## Spring Boot SASL конфігурація

```yaml
# application.yml
spring:
  kafka:
    properties:
      security.protocol: SASL_PLAINTEXT
      sasl.mechanism: PLAIN
      sasl.jaas.config: >
        org.apache.kafka.common.security.plain.PlainLoginModule required
        username="order-producer" password="order-secret";
```

## Файли гілки

```
branch18_security/
├── kafka/
│   ├── kafka_server_jaas.conf    ← SASL users: admin, order-producer, notification-consumer
│   ├── admin.conf                ← admin client properties для kafka-acls CLI
│   └── acl-init.sh               ← створює топік + налаштовує ACL
├── order-service/                ← WRITE-only producer
└── notification-service/
    └── controller/AclProbeController.kt  ← демо ACL violation
```