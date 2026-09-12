#!/bin/bash
set -e

BOOTSTRAP=kafka-b19-1:9092,kafka-b19-2:9092,kafka-b19-3:9092
CONF=/etc/kafka/secrets/admin.conf

echo "=== [INIT] Waiting for all brokers to be ready ==="

create_topic() {
  kafka-topics --bootstrap-server $BOOTSTRAP --command-config $CONF \
    --create --topic "$1" \
    --partitions "${2:-3}" --replication-factor 3 \
    --config min.insync.replicas=2 \
    --if-not-exists
  echo "  topic: $1 (partitions=${2:-3}, rf=3, min.isr=2)"
}

echo ""
echo "=== [INIT] Creating topics ==="
create_topic 19.orders.created
create_topic 19.payments.processed
create_topic 19.payments.failed
create_topic 19.payments.refunded
create_topic 19.inventory.reserved
create_topic 19.inventory.failed
create_topic 19.orders.confirmed
create_topic 19.orders.cancelled
create_topic 19.orders.created.dlt 1

echo ""
echo "=== [INIT] ACL: order-producer ==="
kafka-acls --bootstrap-server $BOOTSTRAP --command-config $CONF \
  --add --allow-principal User:order-producer \
  --operation Write --operation Describe --topic 19.orders.created
kafka-acls --bootstrap-server $BOOTSTRAP --command-config $CONF \
  --add --allow-principal User:order-producer \
  --operation Read --operation Describe --topic 19.payments.failed
kafka-acls --bootstrap-server $BOOTSTRAP --command-config $CONF \
  --add --allow-principal User:order-producer \
  --operation Read --operation Describe --topic 19.inventory.reserved
kafka-acls --bootstrap-server $BOOTSTRAP --command-config $CONF \
  --add --allow-principal User:order-producer \
  --operation Read --operation Describe --topic 19.inventory.failed
kafka-acls --bootstrap-server $BOOTSTRAP --command-config $CONF \
  --add --allow-principal User:order-producer \
  --operation Read --group order-service-group

echo ""
echo "=== [INIT] ACL: payment-processor ==="
kafka-acls --bootstrap-server $BOOTSTRAP --command-config $CONF \
  --add --allow-principal User:payment-processor \
  --operation Read --operation Describe --topic 19.orders.created
kafka-acls --bootstrap-server $BOOTSTRAP --command-config $CONF \
  --add --allow-principal User:payment-processor \
  --operation Read --operation Describe --topic 19.inventory.failed
kafka-acls --bootstrap-server $BOOTSTRAP --command-config $CONF \
  --add --allow-principal User:payment-processor \
  --operation Write --operation Describe --topic 19.payments.processed
kafka-acls --bootstrap-server $BOOTSTRAP --command-config $CONF \
  --add --allow-principal User:payment-processor \
  --operation Write --operation Describe --topic 19.payments.failed
kafka-acls --bootstrap-server $BOOTSTRAP --command-config $CONF \
  --add --allow-principal User:payment-processor \
  --operation Write --operation Describe --topic 19.payments.refunded
kafka-acls --bootstrap-server $BOOTSTRAP --command-config $CONF \
  --add --allow-principal User:payment-processor \
  --operation Read --group payment-service-group

echo ""
echo "=== [INIT] ACL: inventory-worker ==="
kafka-acls --bootstrap-server $BOOTSTRAP --command-config $CONF \
  --add --allow-principal User:inventory-worker \
  --operation Read --operation Describe --topic 19.payments.processed
kafka-acls --bootstrap-server $BOOTSTRAP --command-config $CONF \
  --add --allow-principal User:inventory-worker \
  --operation Write --operation Describe --topic 19.inventory.reserved
kafka-acls --bootstrap-server $BOOTSTRAP --command-config $CONF \
  --add --allow-principal User:inventory-worker \
  --operation Write --operation Describe --topic 19.inventory.failed
kafka-acls --bootstrap-server $BOOTSTRAP --command-config $CONF \
  --add --allow-principal User:inventory-worker \
  --operation Read --group inventory-service-group

echo ""
echo "=== [INIT] ACL: notification-consumer ==="
for topic in 19.orders.created 19.payments.processed 19.payments.failed \
             19.payments.refunded 19.inventory.reserved 19.inventory.failed \
             19.orders.confirmed 19.orders.cancelled; do
  kafka-acls --bootstrap-server $BOOTSTRAP --command-config $CONF \
    --add --allow-principal User:notification-consumer \
    --operation Read --operation Describe --topic "$topic"
done
kafka-acls --bootstrap-server $BOOTSTRAP --command-config $CONF \
  --add --allow-principal User:notification-consumer \
  --operation Read --operation Describe --operation Write --topic 19.orders.created.dlt
kafka-acls --bootstrap-server $BOOTSTRAP --command-config $CONF \
  --add --allow-principal User:notification-consumer \
  --operation Read --group notification-service-group
kafka-acls --bootstrap-server $BOOTSTRAP --command-config $CONF \
  --add --allow-principal User:notification-consumer \
  --operation Read --group notification-service-dlt-group

echo ""
echo "=== [INIT] All configured ACLs ==="
kafka-acls --bootstrap-server $BOOTSTRAP --command-config $CONF --list

echo ""
echo "=== [INIT] Done ==="