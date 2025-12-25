#!/bin/bash
#
# Kafka Topics Creation Script
#
# This script creates the necessary Kafka topics for the Spark Streaming lab.
# It must be run after Kafka is up and running.
#

KAFKA_CONTAINER="kafka"
BOOTSTRAP_SERVER="localhost:9092"

echo "=========================================="
echo "Creating Kafka Topics"
echo "=========================================="
echo ""

# Function to create a topic
create_topic() {
    local topic_name=$1
    local partitions=$2
    local replication_factor=$3

    echo "Creating topic: $topic_name (partitions=$partitions, replication-factor=$replication_factor)"

    docker exec $KAFKA_CONTAINER kafka-topics \
        --create \
        --topic $topic_name \
        --bootstrap-server $BOOTSTRAP_SERVER \
        --partitions $partitions \
        --replication-factor $replication_factor \
        --if-not-exists

    if [ $? -eq 0 ]; then
        echo "✓ Topic '$topic_name' created successfully"
    else
        echo "✗ Failed to create topic '$topic_name'"
    fi
    echo ""
}

# Create topics
create_topic "raw-events" 3 1
create_topic "invalid-events" 1 1

echo "=========================================="
echo "Listing all topics"
echo "=========================================="
docker exec $KAFKA_CONTAINER kafka-topics \
    --list \
    --bootstrap-server $BOOTSTRAP_SERVER

echo ""
echo "✓ Topic creation complete!"
