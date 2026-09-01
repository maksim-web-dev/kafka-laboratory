package com.kafkalab.analytics.controller

import com.kafkalab.analytics.listener.OrderAnalyticsListener
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.math.BigDecimal
import java.math.RoundingMode

@RestController
@RequestMapping("/api/analytics")
class AnalyticsController(private val listener: OrderAnalyticsListener) {

    @GetMapping("/stats")
    fun stats(): Map<String, Any> {
        return mapOf(
            "consumerGroup" to "analytics-service-group",
            "totalOrders" to listener.getTotalOrders(),
            "totalRevenue" to listener.getTotalRevenue().setScale(2, RoundingMode.HALF_UP).toPlainString(),
            "uniqueUsers" to listener.getOrdersByUser().size,
            "ordersByUser" to listener.getOrdersByUser(),
            "revenueByUser" to listener.getRevenueByUser()
                .mapValues { it.value.setScale(2, RoundingMode.HALF_UP).toPlainString() }
        )
    }

    @GetMapping("/stats/user/{userId}")
    fun userStats(@PathVariable userId: String): ResponseEntity<Map<String, Any>> {
        val orders = listener.getOrdersByUser()[userId]
            ?: return ResponseEntity.notFound().build()
        val revenue = listener.getRevenueByUser()[userId] ?: BigDecimal.ZERO
        return ResponseEntity.ok(
            mapOf(
                "userId" to userId,
                "orders" to orders,
                "revenue" to revenue.setScale(2, RoundingMode.HALF_UP).toPlainString()
            )
        )
    }
}