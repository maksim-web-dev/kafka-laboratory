package com.kafkalab.catalog.config

import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.config.TopicConfig
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder

@Configuration
class KafkaTopicConfig {

    /**
     * Log compaction topic for product catalog.
     *
     * cleanup.policy=compact: Kafka retains only the latest value per key.
     * min.cleanable.dirty.ratio=0.01: Start compacting when 1% of log is dirty (aggressive, good for demo).
     * delete.retention.ms=100: How long tombstone (null value) records are retained before deletion.
     * segment.ms=10000: Roll a new log segment every 10s (speeds up compaction in demo).
     */
    @Bean
    fun productsTopic(): NewTopic = TopicBuilder.name("21.products")
        .partitions(3)
        .replicas(1)
        .config(TopicConfig.CLEANUP_POLICY_CONFIG, "compact")
        .config(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.01")
        .config(TopicConfig.DELETE_RETENTION_MS_CONFIG, "100")
        .config(TopicConfig.SEGMENT_MS_CONFIG, "10000")
        .build()
}