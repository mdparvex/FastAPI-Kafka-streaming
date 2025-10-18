#!/bin/bash
set -e

DATA_DIR="/var/lib/kafka/data"
META_FILE="$DATA_DIR/meta.properties"
CLUSTER_ID_FILE="$DATA_DIR/cluster.id"

echo "=========================================="
echo "Starting Kafka KRaft Initialization..."
echo "=========================================="

if [ ! -f "$META_FILE" ]; then
  echo "🔹 Kafka metadata not found. Initializing cluster..."
  if [ ! -f "$CLUSTER_ID_FILE" ]; then
    CLUSTER_ID=$(kafka-storage random-uuid)
    echo "$CLUSTER_ID" > "$CLUSTER_ID_FILE"
    echo "Generated new Cluster ID: $CLUSTER_ID"
  else
    CLUSTER_ID=$(cat "$CLUSTER_ID_FILE")
    echo "Reusing existing Cluster ID: $CLUSTER_ID"
  fi

  kafka-storage format --config /etc/kafka/kraft.properties --cluster-id "$CLUSTER_ID"
else
  echo "Kafka KRaft metadata already exists. Skipping format."
fi

echo "Starting Kafka broker..."
/usr/bin/kafka-server-start /etc/kafka/kraft.properties &

# Wait for Kafka to be ready
echo "Waiting for Kafka broker to start on port 9092..."
while ! nc -z localhost 9092; do
  sleep 1
done

# Additional check to ensure Kafka responds
RETRIES=0
until kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do
  sleep 2
  RETRIES=$((RETRIES+1))
  if [ $RETRIES -ge 15 ]; then
    echo "Kafka broker did not become ready in time."
    exit 1
  fi
done

echo "✅ Kafka broker is ready!"

# Create topic if not exists
TOPIC_NAME="stream-topic"
echo "Ensuring topic '$TOPIC_NAME' exists..."
kafka-topics --create --if-not-exists --topic "$TOPIC_NAME" \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1 || echo "Topic already exists."

echo "Kafka topics created successfully."
echo "=========================================="
echo "Kafka KRaft Mode Fully Started!"
echo "=========================================="

wait
