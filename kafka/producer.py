"""
Kafka Producer - Streams dirty JSON data to Kafka topic

This producer:
1. Reads the events_dirty.json file line by line
2. Sends each line (including malformed ones) to Kafka
3. Simulates real-time streaming with a small delay
4. Does NOT validate or clean the data (that's Spark's job)
"""

import json
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Kafka configuration
# Use 'kafka' hostname when running in Docker, 'localhost' when running locally
import os
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
TOPIC_NAME = 'raw-events'
DATA_FILE = '../data/events_dirty.json'

def create_producer():
    """Create and return a Kafka producer instance"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: v.encode('utf-8'),
            # Retry configuration for reliability
            retries=3,
            acks='all'
        )
        print(f"✓ Connected to Kafka broker at {KAFKA_BROKER}")
        return producer
    except KafkaError as e:
        print(f"✗ Failed to connect to Kafka: {e}")
        raise

def send_events(producer, file_path, delay=0.1):
    """
    Read events from file and send to Kafka

    Args:
        producer: Kafka producer instance
        file_path: Path to the events file
        delay: Delay between messages (simulates streaming)
    """
    count = 0
    errors = 0

    print(f"\n▶ Starting to stream events from {file_path}")
    print(f"▶ Target topic: {TOPIC_NAME}")
    print(f"▶ Streaming delay: {delay}s per message\n")

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue

                try:
                    # Send the raw line as-is (no validation)
                    future = producer.send(TOPIC_NAME, value=line)
                    # Wait for acknowledgment
                    future.get(timeout=10)
                    count += 1

                    # Print progress every 100 messages
                    if count % 100 == 0:
                        print(f"  Sent {count} messages...")

                    # Simulate streaming delay
                    time.sleep(delay)

                except KafkaError as e:
                    errors += 1
                    print(f"✗ Error sending message {line_num}: {e}")

        print(f"\n✓ Streaming complete!")
        print(f"  Total sent: {count}")
        print(f"  Errors: {errors}")

    except FileNotFoundError:
        print(f"✗ File not found: {file_path}")
        raise
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        raise
    finally:
        # Flush and close producer
        producer.flush()
        producer.close()
        print("\n✓ Producer closed")

def main():
    """Main entry point"""
    print("=" * 60)
    print("KAFKA PRODUCER - Dirty Data Streaming")
    print("=" * 60)

    # Create producer
    producer = create_producer()

    # Stream events with 0.1 second delay (adjust as needed)
    send_events(producer, DATA_FILE, delay=0.1)

if __name__ == "__main__":
    main()
