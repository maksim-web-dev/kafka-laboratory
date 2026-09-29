package com.kafkalab.reader.model

data class ProductEvent(
    val productId: String = "",
    val name: String = "",
    val category: String = "",
    val price: Double = 0.0,
    val stock: Int = 0,
    val description: String = ""
)