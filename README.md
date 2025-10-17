# Kafka Streaming Project (FastAPI + Docker + Supervisord)

## Introduction

This project demonstrates a simple **real-time data streaming pipeline** using **Apache Kafka** with **FastAPI** microservices.  
It consists of:

- A **Producer service** - exposes a REST API to send messages to a Kafka topic.
- A **Consumer service** - listens to the same topic and exposes received messages via a REST API.
- **Kafka** and **Zookeeper** - managed via Docker Compose.
- **Supervisord** - ensures automatic service monitoring and restart for stability inside containers.

This setup provides a clean and scalable base for streaming data pipelines or event-driven architectures.

## Project Structure

```css
.
├── docker-compose.yml
├── .env
├── create-topics.sh
│
├── producer/
│   ├── Dockerfile
│   ├── app/
|   |    ├── main.py
│   ├── requirements.txt
│   └── supervisord.conf
│
└── consumer/
    ├── Dockerfile
    ├── app/
    |    ├── main.py
    ├── requirements.txt
    └── supervisord.conf
```

## How It Works

- **Zookeeper** starts first - manages Kafka cluster metadata.
- **Kafka Broker** starts and creates a topic (stream-topic) defined in create-topics.sh.
- **Producer** starts - a FastAPI app running on port 8000, exposing /send_message endpoint to publish data to Kafka.
- **Consumer** starts - another FastAPI app running on port 8001, which consumes Kafka messages and stores them in memory for API access.
- **Supervisord** manages the FastAPI (Uvicorn) process in both producer and consumer containers to ensure resilience and auto-restart.

### Kafka Topic

| **Topic Name** | **Description** |
| --- | --- |
| stream-topic | Main message channel for producer and consumer |

## Environment Configuration (.env)

```bash
KAFKA_BROKER=kafka:9092
KAFKA_CONNECT_MAX_RETRIES=20
KAFKA_CONNECT_RETRY_DELAY=2
TOPIC_NAME=stream-topic

# Kafka internal configurations
KAFKA_BROKER_ID=1
KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181
KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,PLAINTEXT_HOST://0.0.0.0:29092
KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1
KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0

# Zookeeper
ZOOKEEPER_CLIENT_PORT=2181
ZOOKEEPER_TICK_TIME=2000
```

## Running the Project in Docker

### Build and start all services

```bash
docker-compose up --build
```

This will spin up the following containers:

| **Service** | **Description** | **Port** |
| --- | --- | --- |
| zookeeper | Manages Kafka metadata | 2181 |
| kafka | Message broker | 9092 (internal), 29092 (host) |
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
```bash
stream-topic
```
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

The message is now published to the Kafka topic stream-topic.

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

## Supervisord

Each container runs **Supervisord** to manage processes inside Docker:

- Auto-restarts Uvicorn if it crashes.
- Centralized logs are available at /var/log/supervisor/.

**View Logs:**

```bash
docker exec -it producer cat /var/log/supervisor/uvicorn.out.log
docker exec -it consumer cat /var/log/supervisor/uvicorn.out.log
```

## Cleanup

To stop and remove all containers:
```bash
docker-compose down
```
To remove volumes as well:
```bash
docker-compose down -v
```