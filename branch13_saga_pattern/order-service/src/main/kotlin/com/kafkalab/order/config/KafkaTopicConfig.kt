package com.kafkalab.order.config

import org.apache.kafka.clients.admin.NewTopic
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder

@Configuration
class KafkaTopicConfig {

    @Bean fun ordersCreatedTopic(): NewTopic =
        TopicBuilder.name("13.orders.created").partitions(3).replicas(1).build()

    @Bean fun ordersConfirmedTopic(): NewTopic =
        TopicBuilder.name("13.orders.confirmed").partitions(1).replicas(1).build()

    @Bean fun ordersCancelledTopic(): NewTopic =
        TopicBuilder.name("13.orders.cancelled").partitions(1).replicas(1).build()
}