package com.kafkalab.order.config

import org.apache.kafka.clients.admin.NewTopic
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder

@Configuration
class KafkaTopicConfig {

    @Bean fun ordersConfirmedTopic(): NewTopic =
        TopicBuilder.name("19.orders.confirmed").partitions(3).replicas(3)
            .config("min.insync.replicas", "2").build()

    @Bean fun ordersCancelledTopic(): NewTopic =
        TopicBuilder.name("19.orders.cancelled").partitions(3).replicas(3)
            .config("min.insync.replicas", "2").build()
}