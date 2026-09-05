package com.kafkalab.payment.config

import org.apache.kafka.clients.admin.NewTopic
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder

@Configuration
class KafkaTopicConfig {

    @Bean fun paymentsProcessedTopic(): NewTopic =
        TopicBuilder.name("13.payments.processed").partitions(3).replicas(1).build()

    @Bean fun paymentsFailedTopic(): NewTopic =
        TopicBuilder.name("13.payments.failed").partitions(1).replicas(1).build()

    @Bean fun paymentsRefundedTopic(): NewTopic =
        TopicBuilder.name("13.payments.refunded").partitions(1).replicas(1).build()
}