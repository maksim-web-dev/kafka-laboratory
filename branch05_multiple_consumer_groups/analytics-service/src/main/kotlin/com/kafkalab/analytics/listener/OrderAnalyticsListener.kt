package com.kafkalab.analytics.listener

import com.kafkalab.analytics.model.OrderCreatedEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

@Component
class OrderAnalyticsListener {

    private val log = LoggerFactory.getLogger(javaClass)
    private val totalOrders = AtomicInteger(0)
    private val totalRevenueCents = AtomicLong(0)
    private val ordersByUser = ConcurrentHashMap<String, AtomicInteger>()
    private val revenueByUserCents = ConcurrentHashMap<String, AtomicLong>()

    @KafkaListener(topics = ["05.orders.created"], groupId = "analytics-service-group")
    fun handleOrderCreated(record: ConsumerRecord<String, OrderCreatedEvent>) {
        val event = record.value()
        val total = totalOrders.incrementAndGet()
        val amountCents = (event.totalAmount * 100).toLong()
        totalRevenueCents.addAndGet(amountCents)
        ordersByUser.computeIfAbsent(event.userId) { AtomicInteger(0) }.incrementAndGet()
        revenueByUserCents.computeIfAbsent(event.userId) { AtomicLong(0) }.addAndGet(amountCents)

        log.info("[ANALYTICS] Order counted → userId={}, partition={}, offset={}, totalOrders={}",
            event.userId, record.partition(), record.offset(), total)
    }

    fun getTotalOrders(): Int = totalOrders.get()
    fun getTotalRevenue(): BigDecimal = BigDecimal(totalRevenueCents.get()).divide(BigDecimal(100))
    fun getOrdersByUser(): Map<String, Int> = ordersByUser.mapValues { it.value.get() }
    fun getRevenueByUser(): Map<String, BigDecimal> =
        revenueByUserCents.mapValues { BigDecimal(it.value.get()).divide(BigDecimal(100)) }
}