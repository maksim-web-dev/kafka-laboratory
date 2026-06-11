package com.kafkalab.order.config

import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.config.TopicConfig
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder

@Configuration
class KafkaTopicConfig {

    companion object {
        const val TOPIC_ORDERS_CREATED    = "orders.created"
        const val TOPIC_ORDERS_CANCELLED  = "orders.cancelled"
        const val TOPIC_PAYMENTS_PROCESSED = "payments.processed"
        const val TOPIC_NOTIFICATIONS_SENT = "notifications.sent"

        private val RETENTION_7_DAYS_MS = (7L * 24 * 60 * 60 * 1000).toString()
        private val RETENTION_1_DAY_MS  = (1L * 24 * 60 * 60 * 1000).toString()
    }

    // 3 партиції — головний топік для замовлень, retention 7 днів
    @Bean
    fun ordersCreatedTopic(): NewTopic = TopicBuilder.name(TOPIC_ORDERS_CREATED)
        .partitions(3)
        .replicas(1)
        .config(TopicConfig.RETENTION_MS_CONFIG, RETENTION_7_DAYS_MS)
        .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
        .build()

    // 1 партиція — скасування рідкісні, порядок важливий
    @Bean
    fun ordersCancelledTopic(): NewTopic = TopicBuilder.name(TOPIC_ORDERS_CANCELLED)
        .partitions(1)
        .replicas(1)
        .config(TopicConfig.RETENTION_MS_CONFIG, RETENTION_7_DAYS_MS)
        .build()

    // 3 партиції — платежі паралельні, як і замовлення
    @Bean
    fun paymentsProcessedTopic(): NewTopic = TopicBuilder.name(TOPIC_PAYMENTS_PROCESSED)
        .partitions(3)
        .replicas(1)
        .config(TopicConfig.RETENTION_MS_CONFIG, RETENTION_7_DAYS_MS)
        .build()

    // 1 партиція — фінальні сповіщення, retention 1 день
    @Bean
    fun notificationsSentTopic(): NewTopic = TopicBuilder.name(TOPIC_NOTIFICATIONS_SENT)
        .partitions(1)
        .replicas(1)
        .config(TopicConfig.RETENTION_MS_CONFIG, RETENTION_1_DAY_MS)
        .build()
}