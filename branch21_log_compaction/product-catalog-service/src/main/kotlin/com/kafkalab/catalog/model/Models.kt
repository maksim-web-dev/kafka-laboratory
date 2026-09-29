package com.kafkalab.catalog.model

data class ProductEvent(
    val productId: String,
    val name: String,
    val category: String,
    val price: Double,
    val stock: Int = 0,
    val description: String = ""
)

data class CreateProductRequest(
    val name: String,
    val category: String,
    val price: Double,
    val stock: Int = 0,
    val description: String = ""
)

data class UpdateProductRequest(
    val name: String? = null,
    val category: String? = null,
    val price: Double? = null,
    val stock: Int? = null,
    val description: String? = null
)