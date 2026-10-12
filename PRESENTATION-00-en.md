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

# Branch 00
# Preparation & Setup

**Apache Kafka for Certification & Production**

---

## Agenda

1. Operating System requirements
2. Programming language choices
3. Docker Desktop — installation & registration
4. Git & GitHub — registration
5. IDE selection
6. Required tools: Gradle + Git + Docker
7. Step-by-step setup checklist
8. What's next — Branch 01

---

## Operating System

**The course works on all major platforms:**

- **Windows** — Docker Desktop + WSL2 recommended
- **Ubuntu** — native Docker Engine supported
- **CentOS** — native Docker Engine supported
- **macOS** — Docker Desktop (Intel & Apple Silicon)

<br>

> All examples and commands in this course are verified on
> Windows 11, Ubuntu 22.04, and macOS 14 (Sonoma).

---

## Programming Language

**Choose the language you know best:**

- **Kotlin** — primary language used in all examples
- **Java** — all concepts apply 1:1 to Java Spring Boot
- **Python** — `confluent-kafka` library covers most topics
- **C#** — `Confluent.Kafka` NuGet package
- **Others** — any language with a Kafka client library

<br>

> Code examples are in **Kotlin + Spring Boot**.
> The Kafka concepts and configuration are **language-agnostic**.

---

## Docker Desktop — Setup

**Step 1 — Register on Docker Hub:**

```
https://hub.docker.com
  → Sign Up (free account)
  → Verify email
```

**Step 2 — Download Docker Desktop:**

```
https://www.docker.com/products/docker-desktop
  → Choose your OS
  → Install & launch
  → Sign In with Docker Hub credentials
```

> Docker Desktop includes: Docker Engine, Docker Compose, Docker CLI.
> Everything needed to run Kafka locally is bundled here.

---

## Git & GitHub — Setup

**Step 1 — Register on GitHub:**

```
https://github.com
  → Sign Up (free account)
  → Verify email
```

**Step 2 — Install Git locally:**

```bash
# Windows: https://git-scm.com/download/win
# Ubuntu / CentOS:
sudo apt install git        # Ubuntu
sudo yum install git        # CentOS

# macOS:
brew install git
```

> Git is required to clone the course repository and switch between branches.

---

## IDE Selection

**Pick the IDE you are comfortable with:**

- **IntelliJ IDEA** *(recommended for Kotlin/Java)*
  - Community Edition — free, sufficient for this course
  - Ultimate Edition — paid, extra framework support
- **Eclipse** — free, good Java support
- **Visual Studio Code** — free, lightweight, multi-language
- **WebStorm / PyCharm** — JetBrains IDEs for JS / Python

<br>

> IntelliJ IDEA Community Edition is used in all video demos.
> Gradle and Git plugins are **built-in** — no extra configuration needed.

---

## Required Tools — Checklist

**Everything you need before Branch 01:**

```
✓  Docker Desktop  — running & signed in
✓  Git             — installed & configured
✓  IDE             — installed with Gradle support
✓  Docker Hub      — account created
✓  GitHub          — account created
```

<br>

**Verify Docker is working:**

```bash
docker --version
docker compose version
docker run hello-world
```

> If `hello-world` prints "Hello from Docker!" — you are ready to go.

---

## Step-by-Step Setup

**Follow in this order to avoid dependency issues:**

1. **Create Docker Hub account** — needed before installing Docker Desktop
2. **Install Docker Desktop** — sign in with Docker Hub credentials on first launch
3. **Create GitHub account** — needed to clone the repository
4. **Install Git** — configure `user.name` and `user.email`
5. **Install IDE** — open project, let Gradle sync finish

<br>

> **Common pitfall:** Docker Desktop requires WSL2 on Windows.
> Enable it via `wsl --install` before installing Docker Desktop.

---

## Key Takeaways

- **Docker Desktop** is the single tool that runs Kafka, Zookeeper, and all services locally
- **Git** is required to navigate between the 19+ course branches
- **Kotlin + Spring Boot** is the primary stack; all patterns apply to Java
- **IntelliJ IDEA Community** is free and sufficient for the entire course
- **Docker Hub** registration is needed before Docker Desktop sign-in
- **WSL2** is required on Windows — install it first

<br>

> The setup takes ~20 minutes.
> Once done, every branch runs with a single `docker compose up --build`.

---

<!-- _class: title -->

## What's Next — Branch 01

**Basic Producer & Consumer**

- Send your first message to Kafka with Spring Boot
- Consume messages with `@KafkaListener`
- Understand the Producer → Topic → Consumer flow
- Run the full Order → Notification pipeline locally

<br>

# Questions?

**Branch 00 — Preparation & Setup**

*Apache Kafka for Certification & Production*