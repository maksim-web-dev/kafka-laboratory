package com.kafkalab.order.service

import com.kafkalab.order.entity.Order
import com.kafkalab.order.repository.OrderRepository
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import kotlin.random.Random

@Service
class OrderService(private val orderRepository: OrderRepository) {

    private val log = LoggerFactory.getLogger(javaClass)
    private val categories = listOf("ELECTRONICS", "FOOD", "CLOTHING", "GENERAL")

    fun create(userId: String, totalAmount: Double, category: String): Order {
        val order = Order(userId = userId, totalAmount = totalAmount, category = category)
        val saved = orderRepository.save(order)
        log.info("[CDC] Saved order id={} userId={} amount={} — Debezium will capture this change", saved.id, userId, totalAmount)
        return saved
    }

    fun createBatch(users: List<String>, count: Int): Map<String, Any> {
        repeat(count) {
            val user = users[Random.nextInt(users.size)]
            val amount = String.format("%.2f", Random.nextDouble(10.0, 300.0)).toDouble()
            val category = categories.random()
            create(user, amount, category)
        }
        return mapOf("created" to count, "users" to users)
    }

    fun listAll(): List<Order> = orderRepository.findAll()
}