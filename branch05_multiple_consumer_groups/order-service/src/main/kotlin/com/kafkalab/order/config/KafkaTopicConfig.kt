package com.kafkalab.order.config

import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.config.TopicConfig
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder

@Configuration
class KafkaTopicConfig {

    @Bean
    fun ordersCreatedTopic(): NewTopic = TopicBuilder.name("05.orders.created")
        .partitions(3)
        .replicas(1)
        .config(TopicConfig.RETENTION_MS_CONFIG, "604800000")
        .build()

    @Bean
    fun ordersCancelledTopic(): NewTopic = TopicBuilder.name("05.orders.cancelled")
        .partitions(1)
        .replicas(1)
        .config(TopicConfig.RETENTION_MS_CONFIG, "604800000")
        .build()
}