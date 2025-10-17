# producer/main.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kafka import KafkaProducer
import json
import os
import logging
import asyncio
from typing import Optional

logging.basicConfig(level=logging.INFO)
app = FastAPI()

class MessageData(BaseModel):
    message: str
    sender: str

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "stream-topic")

producer: Optional[KafkaProducer] = None

@app.on_event("startup")
async def startup_event():
    global producer
    max_retries = int(os.getenv("KAFKA_CONNECT_MAX_RETRIES", "12"))
    delay = float(os.getenv("KAFKA_CONNECT_RETRY_DELAY", "2"))
    attempt = 0

    while attempt < max_retries:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                api_version_auto_timeout_ms=3000
            )
            logging.info("Kafka producer initialized successfully.")
            return
        except Exception as e:
            attempt += 1
            logging.warning(
                f"Kafka not ready (attempt {attempt}/{max_retries}): {e}. Retrying in {delay}s..."
            )
            await asyncio.sleep(delay)

    logging.error("Exceeded max retries. Could not connect to Kafka on startup.")
    raise RuntimeError("Could not connect to Kafka on startup.")

@app.on_event("shutdown")
async def shutdown_event():
    global producer
    if producer:
        producer.close()
        logging.info("Kafka producer closed.")

@app.post("/send_message")
async def send_message(data: MessageData):
    global producer
    if not producer:
        raise HTTPException(status_code=503, detail="Kafka producer is not ready.")

    try:
        message_json = data.model_dump_json()  # pydantic v2
        # producer.send takes a Python object, value_serializer will handle encode
        producer.send(TOPIC_NAME, value=json.loads(message_json))
        producer.flush(timeout=5)  # ensure it's sent
        logging.info(f"Published message: {message_json}")
        return {"status": "Message sent", "data": data}
    except Exception as e:
        logging.error(f"Failed to send message to Kafka: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to publish message: {e}")
