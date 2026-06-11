package com.kafkalab.order.config

import org.apache.kafka.clients.admin.NewTopic
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder

@Configuration
class KafkaTopicConfig {

    @Bean
    fun ordersCreatedTopic(): NewTopic = TopicBuilder.name("orders.created")
        .partitions(1)
        .replicas(1)
        .build()
}