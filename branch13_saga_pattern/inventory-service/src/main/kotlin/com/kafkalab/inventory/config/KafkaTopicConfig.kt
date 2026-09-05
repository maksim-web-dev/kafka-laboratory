package com.kafkalab.inventory.config

import org.apache.kafka.clients.admin.NewTopic
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder

@Configuration
class KafkaTopicConfig {

    @Bean fun inventoryReservedTopic(): NewTopic =
        TopicBuilder.name("13.inventory.reserved").partitions(3).replicas(1).build()

    @Bean fun inventoryFailedTopic(): NewTopic =
        TopicBuilder.name("13.inventory.failed").partitions(1).replicas(1).build()
}