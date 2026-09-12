package com.kafkalab.inventory.controller

import com.kafkalab.inventory.listener.InventorySagaListener
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/inventory")
class InventoryController(private val listener: InventorySagaListener) {

    @PostMapping("/out-of-stock-next")
    fun outOfStockNext(@RequestParam(defaultValue = "1") count: Int): Map<String, Any> {
        listener.scheduleOutOfStock(count)
        return mapOf(
            "scheduled" to count,
            "message" to "Next $count reservation(s) will fail → saga compensates: PaymentRefunded + OrderCancelled"
        )
    }
}