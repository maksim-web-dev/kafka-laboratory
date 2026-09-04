package com.kafkalab.payment.config

import org.apache.kafka.clients.admin.NewTopic
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder

@Configuration
class KafkaTopicConfig {

    @Bean
    fun paymentsProcessedTopic(): NewTopic =
        TopicBuilder.name("11.payments.processed").partitions(3).replicas(1).build()
}