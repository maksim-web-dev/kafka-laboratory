package com.kafkalab.payment.controller

import com.kafkalab.payment.listener.PaymentSagaListener
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/payments")
class PaymentController(private val listener: PaymentSagaListener) {

    @PostMapping("/fail-next")
    fun failNext(@RequestParam(defaultValue = "1") count: Int): Map<String, Any> {
        listener.scheduleFailure(count)
        return mapOf("scheduled" to count, "message" to "Next $count payment(s) will fail → saga compensation will cancel the order")
    }
}