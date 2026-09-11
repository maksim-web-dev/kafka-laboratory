#!/bin/bash
set -e

BOOTSTRAP=kafka:9092
CONF=/etc/kafka/secrets/admin.conf

echo "=== [ACL-INIT] Creating topic 18.orders.created ==="
kafka-topics --bootstrap-server $BOOTSTRAP \
  --command-config $CONF \
  --create --topic 18.orders.created \
  --partitions 3 --replication-factor 1 \
  --if-not-exists

echo ""
echo "=== [ACL-INIT] ACL: order-producer → WRITE + DESCRIBE on 18.orders.created ==="
kafka-acls --bootstrap-server $BOOTSTRAP \
  --command-config $CONF \
  --add --allow-principal User:order-producer \
  --operation Write --operation Describe \
  --topic 18.orders.created

echo ""
echo "=== [ACL-INIT] ACL: notification-consumer → READ + DESCRIBE on 18.orders.created ==="
kafka-acls --bootstrap-server $BOOTSTRAP \
  --command-config $CONF \
  --add --allow-principal User:notification-consumer \
  --operation Read --operation Describe \
  --topic 18.orders.created

echo ""
echo "=== [ACL-INIT] ACL: notification-consumer → READ on group notification-service-group ==="
kafka-acls --bootstrap-server $BOOTSTRAP \
  --command-config $CONF \
  --add --allow-principal User:notification-consumer \
  --operation Read \
  --group notification-service-group

echo ""
echo "=== [ACL-INIT] All configured ACLs ==="
kafka-acls --bootstrap-server $BOOTSTRAP \
  --command-config $CONF \
  --list

echo ""
echo "=== [ACL-INIT] Done ==="