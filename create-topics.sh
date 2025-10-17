#!/bin/bash
# create-topics.sh
# Wait until Kafka is ready
echo "Waiting for Kafka to be ready..."
cub kafka-ready -b kafka:9092 1 20

echo "Creating topics..."
kafka-topics --create --topic stream-topic --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1 || echo "Topic already exists."

echo "Kafka topics created successfully."
exec /etc/confluent/docker/run
