# Branch 17 — Observability: Prometheus + Grafana + kafka-exporter

## Що демонструє ця гілка

Моніторинг Kafka через стек Prometheus / Grafana з акцентом на **consumer lag** — ключову метрику, яка показує, наскільки консьюмер відстає від продюсера.

## Архітектура

```
order-service  ──►  Kafka (17.orders.created)  ──►  notification-service
                         │
                    kafka-exporter  ──►  Prometheus  ──►  Grafana
                    :9308               :9090           :3017
```

| Сервіс               | Порт  | Роль                              |
|----------------------|-------|-----------------------------------|
| Kafka (KRaft)        | 9120  | Брокер                            |
| kafka-ui             | 8183  | Веб-інтерфейс                     |
| kafka-exporter       | 9308  | Метрики Kafka → Prometheus формат |
| Prometheus           | 9090  | Збір та зберігання метрик         |
| Grafana              | 3017  | Дашборди (admin/admin)            |
| order-service        | 8184  | Продюсер замовлень                |
| notification-service | 8185  | Консьюмер (керується через API)   |

## Запуск

```bash
docker compose -f docker-compose-17.yml up --build
```

## Демонстрація consumer lag

### 1. Призупинити консьюмер

```bash
curl -X POST http://localhost:8185/api/consumer/pause
```

### 2. Відправити пачку повідомлень (flood)

```bash
curl -X POST "http://localhost:8184/api/orders/flood?count=100"
```

### 3. Спостерігати lag у Grafana

Відкрий: http://localhost:3017 → Dashboard **"Kafka Observability"**

Панель **Consumer Lag** покаже зростання. Панель **Lag Over Time** — графік у часі.

### 4. Відновити консьюмер

```bash
curl -X POST http://localhost:8185/api/consumer/resume
```

Lag повинен плавно знизитись до 0.

### 5. Уповільнений режим

```bash
# Консьюмер обробляє 1 повідомлення кожні 3 секунди
curl -X POST "http://localhost:8185/api/consumer/slow?ms=3000"

# Відправити ще повідомлень
curl -X POST "http://localhost:8184/api/orders/flood?count=30"

# Повернути до нормальної швидкості
curl -X POST http://localhost:8185/api/consumer/fast
```

## Ключові метрики kafka-exporter

| Метрика | Опис |
|---------|------|
| `kafka_consumergroup_lag` | Відставання (lag) по партиції |
| `kafka_consumergroup_current_offset` | Поточний offset консьюмера |
| `kafka_topic_partition_current_offset` | Останній offset у партиції |
| `kafka_brokers` | Кількість активних брокерів |

## Чому max.poll.records=1?

```yaml
# notification-service/application.yml
max.poll.records: 1
max.poll.interval.ms: 600000
```

- `max.poll.records=1` — консьюмер бере по 1 повідомленню за раз. Lag змінюється пообіцянково — кожне повідомлення одразу видно на графіку.
- `max.poll.interval.ms=600000` — якщо консьюмер "заснув" (режим pause), Kafka не виключає його з групи до 10 хвилин, тобто партиції не перебалансовуються.

## API

### order-service (8184)

| Метод | URL | Параметри | Опис |
|-------|-----|-----------|------|
| POST | `/api/orders` | `userId`, `totalAmount`, `category` | Відправити одне замовлення |
| POST | `/api/orders/flood` | `count` (default 50) | Швидко відправити N замовлень |

### notification-service (8185)

| Метод | URL | Параметри | Опис |
|-------|-----|-----------|------|
| POST | `/api/consumer/pause` | — | Призупинити обробку |
| POST | `/api/consumer/resume` | — | Відновити обробку |
| POST | `/api/consumer/slow` | `ms` (default 2000) | Уповільнити обробку |
| POST | `/api/consumer/fast` | — | Повна швидкість |

## Grafana Dashboard

Дашборд **"Kafka Observability"** завантажується автоматично через provisioning.

7 панелей:
1. **Consumer Lag (total)** — сумарний lag по всіх партиціях
2. **Messages in Topic** — скільки повідомлень виробив продюсер
3. **Consumer Offset** — де знаходиться консьюмер
4. **Active Brokers** — кількість живих брокерів
5. **Consumer Lag Over Time** — lag у часі (per partition + total)
6. **Producer vs Consumer Offset** — gap між ними = lag
7. **Production Rate** — повідомлень/секунду