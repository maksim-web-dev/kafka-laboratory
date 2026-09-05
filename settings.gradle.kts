rootProject.name = "app"

include("branch01_basic:notification-service")
include("branch01_basic:order-service")

include("branch02_topics_partitions:notification-service")
include("branch02_topics_partitions:order-service")

include("branch03_keys:notification-service")
include("branch03_keys:order-service")

include("branch04_customer_groups:notification-service")
include("branch04_customer_groups:order-service")

include("branch05_multiple_consumer_groups:analytics-service")
include("branch05_multiple_consumer_groups:notification-service")
include("branch05_multiple_consumer_groups:order-service")

include("branch06_offsets_commits:notification-service")
include("branch06_offsets_commits:order-service")

include("branch07_producer_configuration:notification-service")
include("branch07_producer_configuration:order-service")

include("branch08_consumer_configuration:notification-service")
include("branch08_consumer_configuration:order-service")

include("branch09_error_retry:order-service")
include("branch09_error_retry:payment-service")

include("branch10_json_serialization:order-service")
include("branch10_json_serialization:payment-service")
include("branch10_json_serialization:notification-service")

include("branch11_schema_registry:order-service")
include("branch11_schema_registry:payment-service")
include("branch11_schema_registry:notification-service")

include("branch12_idempotent_producer:order-service")
include("branch12_idempotent_producer:payment-service")
include("branch12_idempotent_producer:notification-service")

include("branch13_saga_pattern:order-service")
include("branch13_saga_pattern:payment-service")
include("branch13_saga_pattern:inventory-service")
include("branch13_saga_pattern:notification-service")

include("branch14_streams:order-service")
include("branch14_streams:analytics-service")

include("branch15_connect_cdc_debezium:order-service")

include("branch16_cluster_replication:order-service")
include("branch16_cluster_replication:notification-service")