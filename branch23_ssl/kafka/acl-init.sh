#!/bin/bash
echo "Waiting for Kafka to be ready..."
sleep 15

OPTS="--bootstrap-server kafka:9092 --command-config /etc/kafka/secrets/admin.conf"

# Create topic
kafka-topics $OPTS --create --topic 23.orders.created \
  --partitions 3 --replication-factor 1 2>/dev/null || echo "Topic already exists"

# ACLs for order-producer: WRITE to topic
kafka-acls $OPTS \
  --add --allow-principal User:order-producer \
  --operation Write --operation Describe \
  --topic 23.orders.created

# ACLs for notification-consumer: READ from topic
kafka-acls $OPTS \
  --add --allow-principal User:notification-consumer \
  --operation Read --operation Describe \
  --topic 23.orders.created

# ACLs for notification-consumer: READ consumer group
kafka-acls $OPTS \
  --add --allow-principal User:notification-consumer \
  --operation Read --group notification-service-group

echo "=== ACL setup complete ==="
kafka-acls $OPTS --list