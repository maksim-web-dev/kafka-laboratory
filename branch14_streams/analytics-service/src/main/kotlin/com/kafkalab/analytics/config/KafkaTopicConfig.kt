package com.kafkalab.analytics.config

import org.apache.kafka.clients.admin.NewTopic
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder

@Configuration
class KafkaTopicConfig {

    // Вхідний топік — оголошується тут щоб KafkaAdmin створив його ДО старту Kafka Streams
    @Bean fun ordersCreatedTopic(): NewTopic =
        TopicBuilder.name("14.orders.created").partitions(3).replicas(1).build()

    @Bean fun analyticsOrdersByUserTopic(): NewTopic =
        TopicBuilder.name("14.analytics.orders-by-user").partitions(1).replicas(1).build()

    @Bean fun analyticsOrdersHighValueTopic(): NewTopic =
        TopicBuilder.name("14.analytics.orders-high-value").partitions(1).replicas(1).build()
}