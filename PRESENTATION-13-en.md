---
marp: true
theme: default
paginate: true
backgroundColor: #ffffff
style: |
  section {
    font-family: 'Segoe UI', sans-serif;
    font-size: 28px;
  }
  section.title {
    text-align: center;
    justify-content: center;
  }
  h1 { color: #1a1a2e; }
  h2 { color: #16213e; border-bottom: 2px solid #0f3460; padding-bottom: 8px; }
  code { background: #f4f4f4; padding: 2px 6px; border-radius: 4px; }
  pre { background: #1e1e1e; color: #d4d4d4; border-radius: 8px; }
  blockquote { border-left: 4px solid #0f3460; padding-left: 16px; color: #555; }
  .note { font-size: 20px; color: #666; font-style: italic; }
---

<!-- _class: title -->

# Branch 13
# Saga Pattern (Choreography)

**Apache Kafka for Certification & Production**

---

## Agenda

1. Distributed transactions problem — why 2PC fails
2. Saga Pattern — local transactions + compensation
3. Choreography vs Orchestration
4. Branch13 architecture — 4 services, 8 topics
5. Happy path flow
6. Compensation flow — payment failure
7. Compensation flow — inventory failure
8. Idempotency Key — deduplication in Saga
9. Saga State Machine in order-service
10. Key Takeaways & What's Next

---

## The Distributed Transactions Problem

**Why Two-Phase Commit (2PC) fails in microservices:**

```
2PC (Two-Phase Commit):
  Phase 1: Coordinator asks all participants: "Can you commit?"
  Phase 2: If all say YES → Coordinator sends COMMIT to all

Problems:
  ✗ Blocking protocol — all participants locked during phases
  ✗ Coordinator = Single Point of Failure
  ✗ Poor scalability — synchronous across all services
  ✗ Timeout during phase 2 → inconsistent state (partial commit)

Saga Pattern (alternative):
  ✓ No blocking — each service does a local transaction
  ✓ No SPOF — no central coordinator
  ✓ On failure → compensating transactions undo completed steps
  ! Eventual consistency — not immediate
```

---

## Saga Pattern — Local Transactions + Compensation

**Each step is a local transaction. Failure triggers compensation:**

```
Happy path:
  order.created → payment.processed → inventory.reserved → order.confirmed

Compensation on inventory failure:
  inventory.failed
    → payment.refunded       (undo payment)
    → order.cancelled        (undo order)

Compensation on payment failure:
  payment.failed
    → order.cancelled        (undo order, no payment to refund)
```

<br>

- Each service only knows its own **input topics** and **output topics**
- No service knows the full saga flow — **emergent behavior**
- Compensating transactions **must be idempotent**

---

## Choreography vs Orchestration

**Two approaches to Saga coordination:**

```
Choreography (branch13):
  No central coordinator
  "If I see event X → I publish event Y"
  Each service independently knows its role
  + No SPOF
  + Loose coupling (services only know adjacent topics)
  - Harder to debug (flow is distributed)
  - Flow logic scattered across services

Orchestration (alternative):
  Central saga orchestrator sends commands
  "Step 1: call payment. Step 2: call inventory"
  + Easy to visualize entire flow
  + Easier debugging (all state in one place)
  - Orchestrator = potential SPOF
  - Tighter coupling
```

---

## Architecture — 4 Services, 8 Topics

```
order-service ──[orders.created]──► payment-service
                                          │
                       OK → [payments.processed] ──► inventory-service
                      FAIL → [payments.failed]   ──► order-service (cancel)
                                          │
                         OK → [inventory.reserved] ──► order-service (confirm)
                        FAIL → [inventory.failed]  ──► payment-service (refund)
                                                   └──► order-service (cancel)
                                          │
                              [payments.refunded] ──► notification
```

**notification-service listens to ALL 8 topics** and shows current saga step.

**Ports:** order-service:8081, payment-service:8083, inventory-service:8085,
notification-service:8082, schema-registry:8090, kafka-ui:8080

---

## Happy Path Flow

**All steps succeed — order confirmed:**

```
1. POST /api/orders → order-service
   publishes: 13.orders.created {orderId, userId, amount}
   order state: PENDING

2. payment-service receives orders.created
   processes payment → OK
   publishes: 13.payments.processed {orderId, status=APPROVED}
   order state: PAYMENT_APPROVED

3. inventory-service receives payments.processed
   reserves stock → OK
   publishes: 13.inventory.reserved {orderId}
   order state: INVENTORY_RESERVED

4. order-service receives inventory.reserved
   publishes: 13.orders.confirmed
   order state: CONFIRMED ✓
```

---

## Compensation Flow — Payment Failure

**Payment fails → order cancelled, no inventory involved:**

```
1. order-service publishes: 13.orders.created
   order state: PENDING

2. payment-service receives orders.created
   payment fails (insufficient funds, gateway timeout, ...)
   publishes: 13.payments.failed {orderId, reason}

3. order-service receives payments.failed
   publishes: 13.orders.cancelled
   order state: CANCELLED ✗

notification-service: notifies user "Payment declined"
```

<br>

> Simple compensation: only one step to undo (the order creation).

---

## Compensation Flow — Inventory Failure

**Payment succeeded, inventory fails → refund required:**

```
1. orders.created → payment.processed(APPROVED)
   order state: PAYMENT_APPROVED

2. inventory-service: cannot reserve stock
   publishes: 13.inventory.failed {orderId}

3. payment-service receives inventory.failed
   processes refund
   publishes: 13.payments.refunded {orderId}

4. order-service receives inventory.failed
   publishes: 13.orders.cancelled
   order state: CANCELLED ✗

notification: "Order cancelled. Refund will arrive in 3-5 days."
```

> Two-step compensation: refund payment AND cancel order — must both succeed.

---

## Idempotency Key — Deduplication in Saga

**Kafka at-least-once means duplicates are possible — Saga must handle them:**

```kotlin
// payment-service: protect against duplicate processing
private val processedKeys = ConcurrentHashMap.newKeySet<String>()

fun handleOrderCreated(event: OrderCreatedEvent) {
    val key = "order-created:${event.orderId}"

    if (!processedKeys.add(key)) {
        log.warn("Duplicate detected — skipping: {}", key)
        return   // ← idempotent: second call has no effect
    }

    // process payment...
    kafkaTemplate.send("payments.processed", paymentEvent)
}
```

> `idempotencyKey = "order-created:{orderId}"` — unique per order.
> In production: store processed keys in Redis or DB (not in-memory).

---

## Saga State Machine

**order-service tracks the full saga lifecycle:**

```
                    ┌──────────┐
   POST /api/orders │  PENDING  │
   ──────────────►  └────┬─────┘
                         │ payments.processed (APPROVED)
                    ┌────▼──────────────┐
                    │ PAYMENT_APPROVED   │
                    └────┬──────────────┘
                         │ inventory.reserved
                    ┌────▼──────┐   payments.failed  ┌──────────┐
                    │ CONFIRMED │   or               │ CANCELLED│
                    └───────────┘   inventory.failed └──────────┘

GET /api/orders/{orderId}/status → current state
```

---

## Key Takeaways

- **Saga Pattern** = distributed transactions without blocking, without SPOF
- **Choreography** = services react to events, no central coordinator
- **Compensating transactions** undo completed steps on failure — must be idempotent
- **8 topics for 4 services** = fine-grained event flow, observable in Kafka UI
- **Idempotency key** = essential protection against duplicate processing in at-least-once
- **Avro + Schema Registry** = strict typing enforced across all 4 services
- **notification-service** as observer = each saga step visible in real time

<br>

> Saga: eventual consistency + no blocking = the price of distributed architecture.

---

## What's Next — Branch 14

**Kafka Streams — stateful stream processing**

- `KStream` — unbounded append-only event stream
- `KTable` — stateful upsert view (latest value per key)
- `groupBy / count` — stateful aggregation with State Store (RocksDB)
- Tumbling Windows — fixed-duration time windows
- Interactive Queries — query local State Store via HTTP

---

<!-- _class: title -->

## What's Next — Branch 14
- KStream vs KTable: stream vs state
- State Store: RocksDB-backed, survives restarts
- Tumbling Windows: time-based aggregation
- Interactive Queries: HTTP access to local state

# Questions?

**Branch 13 — Saga Pattern (Choreography)**