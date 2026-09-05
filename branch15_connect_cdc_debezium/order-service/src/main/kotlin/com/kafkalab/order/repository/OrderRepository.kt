package com.kafkalab.order.repository

import com.kafkalab.order.entity.Order
import org.springframework.data.jpa.repository.JpaRepository

interface OrderRepository : JpaRepository<Order, String>