package com.kafkalab.payment.controller

import com.kafkalab.payment.listener.OrderPaymentListener
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/payments")
class PaymentController(private val listener: OrderPaymentListener) {

    @PostMapping("/fail-next")
    fun failNext(@RequestParam(defaultValue = "1") count: Int): Map<String, Any> {
        listener.scheduleFailures(count)
        return mapOf("scheduled" to count, "message" to "Next $count payment(s) will fail")
    }
}