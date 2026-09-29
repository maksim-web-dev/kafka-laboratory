    ## Шаблон-промпт для створення README-{NN}-ukr.md

> Використовуй цей промпт для кожної нової гілки. Підстав `{NN}` → номер гілки (03, 04…),
> `{TOPIC}` → тему гілки (Message Keys, Consumer Groups…), `{PREV}` → попередня гілка.

```
На основі файлів README{NN}.md, kafka_plan.md та analyze.md створи файл README-{NN}-ukr.md
українською мовою з такою структурою (зразок — README-02-ukr.md):

1. # Branch {NN} — {TOPIC}

2. ## `branch{NN}_...` — що вивчаємо
   — bullet-список концепцій (5–10 пунктів) з акцентом на ЩО ВИВЧАЄМО, а не на різниці з минулою гілкою

3. ## Що змінилося порівняно з branch{PREV}
   — bullet-список змін (НЕ таблиця)

4. ## Архітектура
   — ASCII-діаграма потоку даних
   ### Топіки та їх налаштування
   — bullet-список топіків (НЕ таблиця): назва → партиції, retention, призначення

5. ## Ключові концепції цієї гілки
   — по одному підрозділу (###) на кожну нову концепцію
   — code-блоки з прикладами

6. ## Як запустити
   — ТІЛЬКИ одна команда для окремого запуску цієї гілки (Варіанта B — одночасного запуску — немає!)
   ```bash
   docker compose -f docker-compose-{NN}.yml up --build
   ```

7. ## Як протестувати
   — curl-команди з очікуваними відповідями (Linux + Windows варіанти)
   — CLI-команди всередині контейнера kafka

8. ## Як це працює всередині
   — порівняння Producer (що змінилось у Kotlin/Java коді відносно попередньої гілки)
   — порівняння Consumer (що змінилось)
   — тільки ключові фрагменти коду (не весь файл)

9. ## Структура проєкту (зміни відносно branch{PREV})
   — дерево файлів тільки з НОВИМИ або ЗМІНЕНИМИ файлами

10. ## Що далі — branch{NN+1}
    — 3–5 пунктів про наступну тему

---
---

Після подвійного роздільника додай матеріали для курсу Udemy:

11. ## Слайди для презентації (10–12 слайдів)
    Слайд N: [Назва]
    - bullet 1
    - bullet 2

12. ## Текст для презентації (скрипт)
    Слайд N: [текст який говорить викладач — 3–5 речень]

13. ## Тестові питання (до 10 питань)
    **Питання N:** ...
    A) ...  B) ...  C) ...  D) ...
    **Відповідь:** X — пояснення

Вимоги до контенту:
- Всі таблиці → bullet-списки
- Порти — використовувати ТІЛЬКИ стандартизовані значення (змінені раніше для всіх гілок):
    order-service=8081, notification-service=8082, payment-service=8083,
    analytics-service=8084, inventory-service=8085, user-service=8086,
    product-catalog-service=8087, catalog-reader-service=8088, admin-service=8089,
    schema-registry=8090 (зовнішній), kafka-ui=8080, kafka=9092,
    kafka-connect=8083, ksqldb-server=8088, grafana=3000, prometheus=9090,
    postgres=5432, elasticsearch=9200, kibana=5601
  Ці порти повинні бути узгоджені скрізь: curl-команди, docker-compose посилання,
  server.port в application.yml — усе повинно збігатись з цим списком
- "Варіант B" (одночасний запуск) — НЕ додавати
- Технічні ідентифікатори (імена класів, методів, конфіг-ключі) — залишати англійською
- Фокус питань: нові концепції цієї гілки + їх зв'язок з CCDAK-іспитом
- Переноси рядків у прозовому тексті: кожне речення або логічна фраза — з нового рядка
  (не довше ~120 символів), щоб файл зручно читався у IntelliJ IDEA без горизонтального скролу.
  Правило: один абзац = кілька коротких рядків, а не один довгий рядок.
```
-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------

## Шаблон-промпт для створення PRESENTATION-{NN}.pptx

> Підстав `{NN}` → номер гілки, `{TOPIC}` → тема (англійською), `{PREV}` → попередня, `{NEXT}` → наступна.
> Зразок стилю: PRESENTATION-02-en.md / PRESENTATION-02-en.pptx.

```
На основі розділу "## Слайди для презентації" з файлу README-{NN}-ukr.md
та розділу "## Текст для презентації (скрипт)" створи файл PRESENTATION-{NN}.pptx.

Мова презентації: АНГЛІЙСЬКА.
Джерело контенту: README-{NN}-ukr.md (перекласти слайди та скрипт на англійську).
Технічні терміни, імена класів, конфіг-ключі — залишати як є (вони вже англійські).

Проміжний Marp-файл: PRESENTATION-{NN}.md (зберегти поряд з .pptx).
у форматі Marp (сумісний з VS Code Marp extension).

━━━ FRONTMATTER (копіювати без змін для єдиного стилю) ━━━

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

━━━ СТРУКТУРА СЛАЙДІВ ━━━

Слайд 1 — Title (<!-- _class: title -->):
  # Branch {NN}
  # {TOPIC}
  **Apache Kafka for Certification & Production**

Слайд 2 — Agenda (numbered list of topics, 6–9 items, English)

Слайди 3..N-2 — Content (translated from README-{NN}-ukr.md slides):
  - Each slide = one `##` heading + bullets or code block
  - code blocks for: algorithms, comparisons, ASCII diagrams, commands
  - blockquote (>) for key rules and conclusions
  - tables — only if ≤ 4 columns and ≤ 5 rows
  - <br> between logical blocks within a slide
  - No more than 7 bullet points per slide

Second-to-last slide — Key Takeaways:
  - 6–8 bullets, each with a **bold** key term
  - blockquote with the overall conclusion

Last slide — What's Next + Questions (<!-- _class: title -->):
  ## What's Next — Branch {NEXT}
  - 3–4 points about the next topic
  # Questions?
  **Branch {NN} — {TOPIC}**

━━━ FORMATTING RULES ━━━

- Language: ENGLISH throughout (class names, config keys already English — keep as-is)
- Slide separator: ---
- Line length in slides: max ~90 characters
- First word of each bullet capitalized
- Bold (**) for key terms; italic (*) — do not use
- No HTML tags except <br>

━━━ CONVERSION TO .pptx ━━━

After creating the .md file — convert to .pptx using python-pptx.
Script must:
- Parse slides between --- separators
- Slides with <!-- _class: title --> → dark background (#1a1a2e), white text, centered
- Regular slides → heading (## line) as blue bar at top (#0f3460), body below
- code blocks (``` ... ```) → dark rectangle (#1e1e1e), monospace font, grey text
- blockquote (>) → line with blue left border (#0f3460), italic
- bullet points → indented text with •
- **bold** → FontBold=True in python-pptx run
- Save as PRESENTATION-{NN}.pptx
```