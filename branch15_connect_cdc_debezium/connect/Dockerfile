FROM confluentinc/cp-kafka-connect:8.0.1

# Debezium PostgreSQL source connector (CDC)
RUN confluent-hub install --no-prompt debezium/debezium-connector-postgresql:latest

# Elasticsearch sink connector
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-elasticsearch:latest