package com.kafkalab.payment.controller

import com.kafkalab.payment.listener.OrderPaymentListener
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/payments")
class PaymentController(private val listener: OrderPaymentListener) {

    @GetMapping("/stats")
    fun stats(): Map<String, Any> = mapOf(
        "processed" to listener.getProcessedCount(),
        "dlt" to listener.getDltCount(),
        "failNextN" to listener.getFailNextN()
    )

    @PostMapping("/simulate-failure/{count}")
    fun simulateFailure(@PathVariable count: Int): Map<String, Any> {
        listener.setFailNextN(count)
        return mapOf(
            "scheduledFailures" to count,
            "message" to "Next $count invocations will throw PaymentException → DLT → DECLINED event"
        )
    }
}