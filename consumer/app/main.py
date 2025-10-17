# consumer/main.py
from fastapi import FastAPI
from kafka import KafkaConsumer
import json
import os
import logging
import asyncio
from typing import Optional, List

logging.basicConfig(level=logging.INFO)
app = FastAPI()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "stream-topic")

consumer: Optional[KafkaConsumer] = None
messages: List[dict] = []

def consume_messages_sync():
    """
    Blocking consumer loop to be run in a separate thread.
    """
    global consumer, messages
    logging.info("Blocking consumer loop started (thread).")
    try:
        for msg in consumer:
            if msg is None:
                continue
            try:
                messages.append(msg.value)
                logging.info(f"Received message: {msg.value} from topic: {msg.topic}")
            except Exception as e:
                logging.exception("Failed to process message: %s", e)
    except Exception as e:
        logging.exception("Consumer loop terminated with error: %s", e)

@app.on_event("startup")
async def startup_event():
    global consumer
    max_retries = int(os.getenv("KAFKA_CONNECT_MAX_RETRIES", "12"))
    delay = float(os.getenv("KAFKA_CONNECT_RETRY_DELAY", "2"))
    attempt = 0

    while attempt < max_retries:
        try:
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id="fastapi-consumer-group",
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            logging.info("Kafka consumer initialized successfully.")
            break
        except Exception as e:
            attempt += 1
            logging.warning(
                f"Kafka not ready (attempt {attempt}/{max_retries}): {e}. Retrying in {delay}s..."
            )
            await asyncio.sleep(delay)

    if consumer is None:
        logging.error("Exceeded max retries. Could not connect to Kafka on startup.")
        raise RuntimeError("Could not connect to Kafka on startup.")

    # Run blocking consumer loop in a separate thread to avoid blocking event loop
    asyncio.create_task(asyncio.to_thread(consume_messages_sync))

@app.on_event("shutdown")
def shutdown_event():
    global consumer
    if consumer:
        try:
            consumer.close()
            logging.info("Kafka consumer closed.")
        except Exception:
            logging.exception("Error when closing consumer")

@app.get("/messages")
async def get_messages():
    return {"messages": messages}

@app.get("/")
async def root():
    return {"message": "FastAPI consumer service is running."}
