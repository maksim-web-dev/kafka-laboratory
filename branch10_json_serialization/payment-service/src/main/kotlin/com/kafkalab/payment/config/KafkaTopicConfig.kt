package com.kafkalab.payment.config

import org.apache.kafka.clients.admin.NewTopic
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder

@Configuration
class KafkaTopicConfig {

    @Bean
    fun paymentsProcessedTopic(): NewTopic = TopicBuilder.name("10.payments.processed")
        .partitions(3)
        .replicas(1)
        .build()

    @Bean
    fun ordersCreatedRetry0(): NewTopic = TopicBuilder.name("10.orders.created-retry-0")
        .partitions(3)
        .replicas(1)
        .build()

    @Bean
    fun ordersCreatedRetry1(): NewTopic = TopicBuilder.name("10.orders.created-retry-1")
        .partitions(3)
        .replicas(1)
        .build()

    @Bean
    fun ordersCreatedDlt(): NewTopic = TopicBuilder.name("10.orders.created-dlt")
        .partitions(3)
        .replicas(1)
        .build()
}