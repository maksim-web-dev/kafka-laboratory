package com.kafkalab.user.controller

import com.kafkalab.user.model.ProductEvent
import com.kafkalab.user.model.UpsertUserRequest
import com.kafkalab.user.service.UserService
import org.springframework.web.bind.annotation.*
import java.util.UUID

@RestController
@RequestMapping("/api/users")
class UserController(private val userService: UserService) {

    @PostMapping
    fun upsertUser(@RequestBody req: UpsertUserRequest): Any {
        val userId = "user-${UUID.randomUUID().toString().take(8)}"
        return userService.upsert(req, userId)
    }

    @DeleteMapping("/{userId}")
    fun deleteUser(@PathVariable userId: String): Map<String, String> {
        userService.delete(userId)
        return mapOf("status" to "tombstone_sent", "userId" to userId)
    }

    @PostMapping("/products")
    fun publishProduct(@RequestBody req: ProductEvent) = userService.publishProduct(req)
}