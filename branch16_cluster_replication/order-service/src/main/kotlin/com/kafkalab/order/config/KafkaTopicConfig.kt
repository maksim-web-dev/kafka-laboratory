package com.kafkalab.order.config

import org.apache.kafka.clients.admin.NewTopic
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder

@Configuration
class KafkaTopicConfig {

    @Bean
    fun ordersCreatedTopic(): NewTopic =
        TopicBuilder.name("16.orders.created")
            .partitions(3)
            .replicas(3)
            // At least 2 ISR must acknowledge — producer fails if only 1 broker alive
            .config("min.insync.replicas", "2")
            .build()
}