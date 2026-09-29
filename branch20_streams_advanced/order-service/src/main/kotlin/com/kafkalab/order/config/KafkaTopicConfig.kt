package com.kafkalab.order.config

import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.config.TopicConfig
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder

@Configuration
class KafkaTopicConfig {

    @Bean fun ordersCreatedTopic(): NewTopic =
        TopicBuilder.name("20.orders.created").partitions(3).replicas(1).build()

    @Bean fun paymentsProcessedTopic(): NewTopic =
        TopicBuilder.name("20.payments.processed").partitions(3).replicas(1).build()

    @Bean fun usersTopic(): NewTopic =
        TopicBuilder.name("20.users").partitions(3).replicas(1).build()

    @Bean fun productsTopic(): NewTopic =
        TopicBuilder.name("20.products")
            .partitions(3).replicas(1)
            .config(TopicConfig.CLEANUP_POLICY_CONFIG, "compact")
            .build()

    @Bean fun enrichedOrdersTopic(): NewTopic =
        TopicBuilder.name("20.enriched.orders").partitions(3).replicas(1).build()

    @Bean fun enrichedProductsTopic(): NewTopic =
        TopicBuilder.name("20.enriched.products").partitions(3).replicas(1).build()

    @Bean fun matchedOrdersTopic(): NewTopic =
        TopicBuilder.name("20.matched.orders").partitions(3).replicas(1).build()
}