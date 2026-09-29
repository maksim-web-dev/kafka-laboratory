# Аналіз: чому orderId — поганий ключ (коректне пояснення)

## Помилка в README-03-ukr.md

У розділі "### Чому orderId — поганий ключ" написано:

> "Якщо після OrderCreated надходить OrderCancelled для того самого замовлення,
> вони можуть опинитися в різних партиціях → consumer обробить Cancelled РАНІШЕ Created."

**Це неправильно.** Якщо обидва events мають один і той самий `orderId` як ключ —
`hash(orderId) % 3` дасть той самий результат для обох. Обидва потраплять в **одну партицію**.

---

## Справжня проблема з orderId як ключем

Проблема виникає не всередині одного замовлення, а **між замовленнями одного користувача**.

### Сценарій з orderId як ключем

Користувач `user-42` робить три замовлення:

```
OrderCreated(orderId=order-A, userId=user-42) → hash("order-A") % 3 = 2  → partition 2
OrderCreated(orderId=order-B, userId=user-42) → hash("order-B") % 3 = 0  → partition 0
OrderCancelled(orderId=order-A, userId=user-42) → hash("order-A") % 3 = 2 → partition 2
```

- OrderCreated і OrderCancelled для **одного** замовлення — в одній партиції ✅
- Але OrderCreated(order-A) і OrderCreated(order-B) — в **різних** партиціях

Якщо в бізнес-логіці є залежність між замовленнями одного користувача (кредитний ліміт,
програма лояльності, saga з компенсацією на рівні акаунта) — порядок між order-A і order-B
**не гарантований**.

### Сценарій з userId як ключем

```
OrderCreated(orderId=order-A, userId=user-42) → hash("user-42") % 3 = 1  → partition 1
OrderCreated(orderId=order-B, userId=user-42) → hash("user-42") % 3 = 1  → partition 1
OrderCancelled(orderId=order-A, userId=user-42) → hash("user-42") % 3 = 1 → partition 1
```

Всі три events для `user-42` → **одна партиція** → строгий порядок між ними гарантований.

---

## Друга причина: consumer groups (актуально з branch04)

Коли запущено кілька екземплярів notification-service:

### З orderId як ключем (3 consumers, 3 partitions)

```
consumer-1 читає partition 0: [OrderCreated(order-B)]
consumer-2 читає partition 1: []
consumer-3 читає partition 2: [OrderCreated(order-A), OrderCancelled(order-A)]
```

Events одного **замовлення** (order-A) — в одного consumer. ✅  
Але events різних **замовлень** одного user-42 — у різних consumers. Немає гарантії порядку
між `order-A` і `order-B` на рівні користувача.

### З userId як ключем

```
consumer-1 читає partition 0: [всі events user-99]
consumer-2 читає partition 1: [всі events user-42: order-A created, order-B created, order-A cancelled]
consumer-3 читає partition 2: [всі events user-17]
```

Всі events `user-42` завжди обробляє **один і той самий** consumer-екземпляр → гарантований порядок.

---

## Підсумок

| Питання | orderId як ключ | userId як ключ |
|---------|----------------|----------------|
| OrderCreated і OrderCancelled для ТОГО САМОГО замовлення | Одна партиція ✅ | Одна партиція ✅ |
| Різні замовлення одного користувача | Різні партиції ❌ | Одна партиція ✅ |
| У multi-consumer сценарії (branch04+) | User-events розкидані між consumers ❌ | Всі user-events у одного consumer ✅ |

**Висновок:** `orderId` не ламає порядок `Created → Cancelled` для конкретного замовлення.
Але він не дає гарантій порядку між різними замовленнями одного користувача.
`userId` — правильніший вибір ключа коли важлива послідовність усіх дій конкретного entity (user).

---

## Що виправити в README-03-ukr.md

Розділ "### Чому orderId — поганий ключ" треба переписати з акцентом на:
1. Проблема НЕ в тому що Created і Cancelled одного замовлення потраплять у різні партиції
2. Проблема В ТОМУ що різні замовлення одного user'а розкидаються по різних партиціях
3. В multi-consumer сценарії це означає що різні events одного user'а обробляються різними consumer-instances без гарантії порядку між ними