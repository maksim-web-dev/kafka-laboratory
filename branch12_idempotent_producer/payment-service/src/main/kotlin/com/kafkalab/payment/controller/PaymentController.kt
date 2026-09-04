package com.kafkalab.payment.controller

import com.kafkalab.payment.listener.OrderPaymentListener
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/payments")
class PaymentController(private val listener: OrderPaymentListener) {

    @PostMapping("/abort-next")
    fun abortNext(@RequestParam(defaultValue = "1") count: Int): Map<String, Any> {
        listener.scheduleAbort(count)
        return mapOf(
            "scheduled" to count,
            "message" to "Next $count payment transaction(s) will be aborted — read_committed consumers will NOT see the message"
        )
    }
}