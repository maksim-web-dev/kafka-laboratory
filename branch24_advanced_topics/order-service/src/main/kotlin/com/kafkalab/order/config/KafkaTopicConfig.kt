package com.kafkalab.order.config

import org.apache.kafka.clients.admin.NewTopic
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder

@Configuration
class KafkaTopicConfig {

    @Bean fun ordersCreatedTopic(): NewTopic =
        TopicBuilder.name("24.orders.created").partitions(3).replicas(1).build()

    @Bean fun ordersBenchmarkTopic(): NewTopic =
        TopicBuilder.name("24.orders.benchmark").partitions(3).replicas(1).build()
}