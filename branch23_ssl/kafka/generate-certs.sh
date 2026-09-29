#!/bin/bash
# Generate SSL certificates for Kafka SASL_SSL demo (branch23)

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CERTS_DIR="$SCRIPT_DIR/certs"
mkdir -p "$CERTS_DIR"
PASSWORD="kafkalab-ssl-secret"

echo "=== Step 1: Create Certificate Authority (CA) ==="
openssl req -new -x509 -keyout "$CERTS_DIR/ca-key" -out "$CERTS_DIR/ca-cert" -days 365 \
  -subj "/CN=KafkaLab-CA/OU=KafkaLab/O=KafkaLab/L=Kyiv/ST=Ukraine/C=UA" \
  -passout pass:$PASSWORD

echo "=== Step 2: Create Kafka Broker Keystore ==="
keytool -keystore "$CERTS_DIR/kafka.server.keystore.jks" -alias localhost \
  -keyalg RSA -validity 365 -genkey -storepass $PASSWORD -keypass $PASSWORD \
  -dname "CN=kafka,OU=KafkaLab,O=KafkaLab,L=Kyiv,ST=Ukraine,C=UA"

echo "=== Step 3: Sign broker certificate with CA ==="
keytool -keystore "$CERTS_DIR/kafka.server.keystore.jks" -alias localhost \
  -certreq -file "$CERTS_DIR/cert-file" -storepass $PASSWORD

openssl x509 -req -CA "$CERTS_DIR/ca-cert" -CAkey "$CERTS_DIR/ca-key" \
  -in "$CERTS_DIR/cert-file" -out "$CERTS_DIR/cert-signed" -days 365 \
  -CAcreateserial -passin pass:$PASSWORD

keytool -keystore "$CERTS_DIR/kafka.server.keystore.jks" -alias CARoot \
  -import -file "$CERTS_DIR/ca-cert" -storepass $PASSWORD -noprompt

keytool -keystore "$CERTS_DIR/kafka.server.keystore.jks" -alias localhost \
  -import -file "$CERTS_DIR/cert-signed" -storepass $PASSWORD -noprompt

echo "=== Step 4: Create Server Truststore ==="
keytool -keystore "$CERTS_DIR/kafka.server.truststore.jks" \
  -alias CARoot -import -file "$CERTS_DIR/ca-cert" \
  -storepass $PASSWORD -noprompt

echo "=== Step 5: Create Client Truststore ==="
keytool -keystore "$CERTS_DIR/kafka.client.truststore.jks" \
  -alias CARoot -import -file "$CERTS_DIR/ca-cert" \
  -storepass $PASSWORD -noprompt

echo ""
echo "=== Done! Certificates generated in $CERTS_DIR ==="
echo "Password for all stores: $PASSWORD"
echo ""
echo "Files created:"
ls -la "$CERTS_DIR/"