package com.kafkalab.order.entity

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.LocalDateTime
import java.util.UUID

@Entity
@Table(name = "orders")
class Order(
    @Id
    val id: String = UUID.randomUUID().toString(),

    @Column(name = "user_id", nullable = false)
    val userId: String,

    @Column(name = "total_amount", nullable = false)
    val totalAmount: Double,

    @Column(nullable = false)
    val category: String,

    @Column(nullable = false)
    val status: String = "PENDING",

    @Column(name = "created_at", nullable = false)
    val createdAt: LocalDateTime = LocalDateTime.now()
)