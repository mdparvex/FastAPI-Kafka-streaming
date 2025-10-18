# Kafka Streaming Project (FastAPI + Docker + KRaft Mode Kafka)

## Introduction

This project demonstrates a simple **real-time data streaming pipeline** using **Apache Kafka (KRaft mode)** with **FastAPI** microservices.  
It consists of:

- A **Producer service** - exposes a REST API to send messages to a Kafka topic.
- A **Consumer service** - listens to the same topic and exposes received messages via a REST API.
- **Kafka Broker (KRaft mode)** - no Zookeeper required, managed via Docker Compose.
- **Supervisord** - ensures automatic service monitoring and restart for stability inside containers.

This setup provides a clean and scalable base for streaming data pipelines or event-driven architectures.

---

## Project Structure

```text
.
├── docker-compose.yaml
├── .env
├── kraft-entrypoint.sh
├── config/
│   └── kafka-kraft.properties
│
├── producer/
│   ├── Dockerfile
│   ├── app/
│   │    └── main.py
│   ├── requirements.txt
│   └── supervisord.conf
│
└── consumer/
    ├── Dockerfile
    ├── app/
    │    └── main.py
    ├── requirements.txt
    └── supervisord.conf
```

---

## How It Works

- **Kafka Broker (KRaft mode)** starts first and auto-initializes the cluster.
- Cluster metadata and Cluster ID are persisted in `/var/lib/kafka/data`.
- **Producer** starts - FastAPI app on port 8000, exposing `/send_message` endpoint to publish data to Kafka.
- **Consumer** starts - FastAPI app on port 8001, consuming Kafka messages and storing them in memory for API access.
- **Supervisord** manages the FastAPI (Uvicorn) process in both producer and consumer containers to ensure resilience and auto-restart.
- Kafka topics are created automatically via `kraft-entrypoint.sh` after broker is ready.

---

### Kafka Topic

| **Topic Name** | **Description** |
| --- | --- |
| stream-topic | Main message channel for producer and consumer |

---

## Environment Configuration (.env)

```bash
KAFKA_BROKER=kafka:9092
KAFKA_CONNECT_MAX_RETRIES=20
KAFKA_CONNECT_RETRY_DELAY=2
TOPIC_NAME=stream-topic
```

**Note:** No Zookeeper is required in KRaft mode. All legacy Zookeeper variables have been removed.

---

## Running the Project in Docker

### Build and start all services

```bash
docker-compose up --build
```

This will spin up the following containers:

| **Service** | **Description** | **Port** |
| --- | --- | --- |
| kafka | Kafka Broker (KRaft mode) | 9092 (internal), 29092 (host) |
| producer | FastAPI producer service | 8000 |
| consumer | FastAPI consumer service | 8001 |

### Check logs (optional)

```bash
docker-compose logs -f producer
docker-compose logs -f consumer
```

### Verify Kafka topic creation

```bash
docker exec -it kafka kafka-topics --list --bootstrap-server kafka:9092
```

You should see:

```text
stream-topic
```

---

## API Endpoints

### Producer API

**Endpoint:**
```bash
POST http://localhost:8000/send_message
```

**Example Request Body:**

```json
{
  "message": "Hello from FastAPI Producer!",
  "sender": "Producer-Service"
}
```

**Example Response:**

```json
{
  "status": "Message sent",
  "data": {
    "message": "Hello from FastAPI Producer!",
    "sender": "Producer-Service"
  }
}
```

The message is now published to the Kafka topic `stream-topic`.

---

### Consumer API

**Endpoint:**

```bash
GET http://localhost:8001/messages
```

**Example Response (after a message is published):**

```json
{
  "messages": [
    {
      "message": "Hello from FastAPI Producer!",
      "sender": "Producer-Service"
    }
  ]
}
```

This confirms the consumer received and stored the message from Kafka.

---

## Supervisord

Each container runs **Supervisord** to manage processes inside Docker:

- Auto-restarts Uvicorn if it crashes.
- Centralized logs are available at `/var/log/supervisor/`.

**View Logs:**

```bash
docker exec -it producer cat /var/log/supervisor/uvicorn.out.log
docker exec -it consumer cat /var/log/supervisor/uvicorn.out.log
```

---

## Cleanup

To stop and remove all containers:

```bash
docker-compose down
```

To remove volumes as well:

```bash
docker-compose down -v
```

---