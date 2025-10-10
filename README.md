# 🧩 Popov R&D — Spring Boot RabbitMQ Example

This repository demonstrates a **multi-module Maven project** built with **Spring Boot 3.x** to showcase reliable, observable messaging with **RabbitMQ**.

It includes two independent applications that communicate through a single RabbitMQ broker:

| Module | Description |
|---------|--------------|
| 📨 **producer** | Publishes `JobMessage` objects as JSON with full reliability (retries, confirms, returns). |
| 📥 **consumer** | Listens for messages, processes them, and supports Dead-Letter Queue (DLQ) handling. |

---

Each submodule can be run separately but shares common settings and version alignment via the parent POM.

---

## 🧰 Prerequisites

- **JDK 21 (LTS)**
- **Maven 3.9+**
- **Docker** — required to run RabbitMQ locally
- **RabbitMQ image:** `rabbitmq:4-management` (includes Web UI and Prometheus plugin)

---

## 🐇 Start RabbitMQ with Docker Compose

For local development, use the included `docker-compose.yml`:

```
yaml
version: "3.8"
services:
  rabbitmq:
    image: "rabbitmq:4-management"
    container_name: rabbitmq-local
    ports:
      - "5672:5672"     # AMQP protocol (for clients)
      - "15672:15672"   # HTTP management UI
    environment:
      RABBITMQ_DEFAULT_USER: guest
      RABBITMQ_DEFAULT_PASS: guest
    volumes:
      - rabbitmq_data:/var/lib/rabbitmq

volumes:
  rabbitmq_data:
    
```

## 🚀 Launch Applications

```
# From project root
mvn clean package

# Start producer
mvn -pl producer spring-boot:run

# Start consumer
mvn -pl consumer spring-boot:run
```

## 📨 Producer Module

This module demonstrates a **Spring Boot 3.x producer** that sends JSON messages to RabbitMQ with full reliability and observability.

---

### ⚙️ Features

- ✅ Publishes `JobMessage` objects as JSON using `Jackson2JsonMessageConverter`
- ✅ Supports **publisher confirms** and **returns** for broker-side delivery guarantees
- ✅ Enables **client-side retry** for transient connection errors
- ✅ Catches and logs `AmqpException` when the app cannot send after all retries
- ✅ Provides optional **fallback storage** (DB/File) for failed messages
- ✅ Uses environment variables for flexible configuration (`RABBIT_USER`, `RABBIT_PASS`, etc.)

---

## 📥 Consumer Module

This module demonstrates a **Spring Boot 3.x consumer** designed for dependable message processing, controlled acknowledgments, and built-in dead-letter support.

---

### ⚙️ Features

- ✅ **Manual acknowledgment mode** — messages are explicitly `ACK`ed or `NACK`ed after processing, ensuring no loss if the app crashes mid-task.
- ✅ **Quorum queue support** — uses durable Raft-replicated queues with optional DLQ routing for failed deliveries.
- ✅ **Redelivery control** — via `x-delivery-limit` and DLX, automatically moves messages to a `.dlq` queue after multiple failed attempts.
- ✅ **Thread-safe concurrency** — configured with a bounded consumer thread pool and `prefetch` to balance throughput and memory use.
- ✅ **Structured logging** — every delivery includes correlation ID, message ID, and timestamps for full traceability.
- ✅ **Graceful back-pressure** — prevents overload by limiting unacked (in-flight) messages per consumer channel.

---

### 🧭 Observability

Producer-side metrics can be exposed via Spring Boot Actuator, e.g.
- Network connection failures before broker;
- Mapping app-level job types to messages.

/actuator/prometheus — exposes Micrometer metrics

## 🖥️ Broker Monitoring
In fact, many production setups drop application-level counters entirely once they have proper broker monitoring.

RabbitMQ Management Plugin (enabled by default in the Docker image):

```
docker run -d \
--name rabbitmq \
-p 5672:5672 -p 15672:15672 \
rabbitmq:3-management
```

UI: http://localhost:15672

Prometheus metrics endpoint (optional): :15692

Track:

- Publish rate & confirm rate
- Queue depth & message latency
- ACK/NACK stats
- Connection and channel counts