"""
Spark Structured Streaming Application - Advanced Processing with Data Quality

This application demonstrates:
1. Handling dirty streaming data without crashes
2. Event-time processing with watermarks
3. Windowed aggregations with state management
4. Separating valid and invalid events
5. Multiple output sinks
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, when, current_timestamp, window,
    avg, count, to_json, struct, lit
)
from pyspark.sql.types import StringType
from schema import event_schema, VALID_COUNTRIES, MIN_VALID_TEMPERATURE, MAX_VALID_TEMPERATURE, INVALID_TEMPERATURE_MARKER

# Kafka configuration
# Use 'kafka' hostname when running in Docker, 'localhost' when running locally
import os
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
INPUT_TOPIC = "raw-events"
INVALID_EVENTS_TOPIC = "invalid-events"

# Watermark configuration
WATERMARK_DELAY = "10 minutes"

# Window configuration
WINDOW_DURATION = "5 minutes"
WINDOW_SLIDE = "1 minute"


def create_spark_session():
    """Create and configure Spark session with Kafka support"""
    spark = SparkSession.builder \
        .appName("AdvancedSparkStructuredStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0") \
        .config("spark.sql.streaming.checkpointLocation", "./checkpoint") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("✓ Spark session created")
    return spark


def read_kafka_stream(spark):
    """Read raw streaming data from Kafka"""
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    print("✓ Connected to Kafka stream")
    return df


def parse_and_validate_events(raw_df):
    """
    Parse JSON and separate valid from invalid events

    Returns:
        valid_df: DataFrame with valid, clean events
        invalid_df: DataFrame with invalid events (for monitoring)
    """
    # Extract the value as string
    df = raw_df.selectExpr("CAST(value AS STRING) as json_string", "timestamp as kafka_timestamp")

    # Try to parse JSON with the expected schema
    parsed_df = df.withColumn(
        "parsed",
        from_json(col("json_string"), event_schema)
    )

    # Separate valid JSON structure from malformed JSON
    valid_structure_df = parsed_df.filter(col("parsed").isNotNull())
    malformed_json_df = parsed_df.filter(col("parsed").isNull())

    # Expand the parsed struct into columns
    expanded_df = valid_structure_df.select(
        col("json_string"),
        col("kafka_timestamp"),
        col("parsed.device_id").alias("device_id"),
        col("parsed.event_time").alias("event_time"),
        col("parsed.temperature").alias("temperature"),
        col("parsed.country").alias("country")
    )

    # Apply business rules for validation
    validated_df = expanded_df.withColumn(
        "is_valid",
        # All conditions must be true for a valid event
        (col("device_id").isNotNull()) &
        (col("device_id") != "") &
        (col("event_time").isNotNull()) &
        (col("country").isNotNull()) &
        (col("country") != "") &
        (col("country") != "123") &  # Invalid country code
        (~col("country").like("%@%")) &  # Invalid characters
        (
            col("temperature").isNull() |  # Temperature is optional
            (
                (col("temperature") >= MIN_VALID_TEMPERATURE) &
                (col("temperature") <= MAX_VALID_TEMPERATURE) &
                (col("temperature") != INVALID_TEMPERATURE_MARKER)
            )
        )
    )

    # Separate valid and invalid events
    valid_events = validated_df.filter(col("is_valid") == True).drop("is_valid")
    invalid_events = validated_df.filter(col("is_valid") == False).drop("is_valid")

    # Add reason for invalidity
    invalid_events = invalid_events.withColumn(
        "invalid_reason",
        when(col("device_id").isNull() | (col("device_id") == ""), "Missing device_id")
        .when(col("event_time").isNull(), "Missing event_time")
        .when(col("country").isNull() | (col("country") == ""), "Missing country")
        .when(col("country") == "123", "Invalid country code")
        .when(col("country").like("%@%"), "Invalid country format")
        .when(col("temperature") == INVALID_TEMPERATURE_MARKER, "Invalid temperature marker")
        .when(
            (col("temperature") < MIN_VALID_TEMPERATURE) |
            (col("temperature") > MAX_VALID_TEMPERATURE),
            "Temperature out of range"
        )
        .otherwise("Other validation failure")
    )

    # Handle malformed JSON
    invalid_json = malformed_json_df.select(
        col("json_string"),
        col("kafka_timestamp"),
        lit(None).cast(StringType()).alias("device_id"),
        lit(None).cast(StringType()).alias("event_time"),
        lit(None).cast(StringType()).alias("temperature"),
        lit(None).cast(StringType()).alias("country"),
        lit("Malformed JSON").alias("invalid_reason")
    )

    # Union all invalid events
    all_invalid = invalid_events.union(invalid_json)

    print("✓ Event parsing and validation configured")
    return valid_events, all_invalid


def perform_windowed_aggregations(valid_df):
    """
    Perform event-time windowed aggregations

    Aggregations:
    1. Average temperature per device per window
    2. Event count per country per window
    """
    # Set watermark for late data handling
    windowed_df = valid_df \
        .withWatermark("event_time", WATERMARK_DELAY)

    # Aggregation 1: Average temperature per device per window
    device_agg = windowed_df \
        .filter(col("temperature").isNotNull()) \
        .groupBy(
            window(col("event_time"), WINDOW_DURATION, WINDOW_SLIDE),
            col("device_id")
        ) \
        .agg(
            avg("temperature").alias("avg_temperature"),
            count("*").alias("event_count")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("device_id"),
            col("avg_temperature"),
            col("event_count")
        )

    # Aggregation 2: Event count per country per window
    country_agg = windowed_df \
        .groupBy(
            window(col("event_time"), WINDOW_DURATION, WINDOW_SLIDE),
            col("country")
        ) \
        .agg(
            count("*").alias("event_count"),
            avg("temperature").alias("avg_temperature")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("country"),
            col("event_count"),
            col("avg_temperature")
        )

    print("✓ Windowed aggregations configured")
    return device_agg, country_agg


def write_to_console(df, query_name, output_mode="append"):
    """Write streaming DataFrame to console for debugging"""
    query = df.writeStream \
        .outputMode(output_mode) \
        .format("console") \
        .option("truncate", "false") \
        .queryName(query_name) \
        .start()

    print(f"✓ Console sink started: {query_name}")
    return query


def write_to_files(df, path, query_name, output_mode="append"):
    """Write streaming DataFrame to Parquet files"""
    query = df.writeStream \
        .outputMode(output_mode) \
        .format("parquet") \
        .option("path", path) \
        .option("checkpointLocation", f"./checkpoint/{query_name}") \
        .queryName(query_name) \
        .start()

    print(f"✓ File sink started: {query_name} -> {path}")
    return query


def write_to_kafka(df, topic, query_name):
    """Write streaming DataFrame back to Kafka"""
    # Convert DataFrame to JSON string for Kafka
    kafka_df = df.select(
        to_json(struct([col(c) for c in df.columns])).alias("value")
    )

    query = kafka_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", topic) \
        .option("checkpointLocation", f"./checkpoint/{query_name}") \
        .queryName(query_name) \
        .start()

    print(f"✓ Kafka sink started: {query_name} -> {topic}")
    return query


def main():
    """Main application entry point"""
    print("=" * 70)
    print("SPARK STRUCTURED STREAMING - Advanced Processing")
    print("=" * 70)
    print()

    # 1. Create Spark session
    spark = create_spark_session()

    # 2. Read from Kafka
    raw_stream = read_kafka_stream(spark)

    # 3. Parse and validate events
    valid_events, invalid_events = parse_and_validate_events(raw_stream)

    # 4. Perform windowed aggregations
    device_aggregations, country_aggregations = perform_windowed_aggregations(valid_events)

    # 5. Set up output sinks
    print()
    print("Setting up output sinks...")
    print("-" * 70)

    queries = []

    # Console outputs (for debugging)
    queries.append(write_to_console(valid_events, "valid_events_console", "append"))
    queries.append(write_to_console(invalid_events, "invalid_events_console", "append"))
    queries.append(write_to_console(device_aggregations, "device_agg_console", "update"))
    queries.append(write_to_console(country_aggregations, "country_agg_console", "update"))

    # File outputs (for persistence)
    queries.append(write_to_files(valid_events, "./output/valid_events", "valid_events_files", "append"))
    queries.append(write_to_files(invalid_events, "./output/invalid_events", "invalid_events_files", "append"))
    queries.append(write_to_files(device_aggregations, "./output/device_agg", "device_agg_files", "append"))
    queries.append(write_to_files(country_aggregations, "./output/country_agg", "country_agg_files", "append"))

    # Kafka output (for invalid events monitoring)
    queries.append(write_to_kafka(invalid_events, INVALID_EVENTS_TOPIC, "invalid_events_kafka"))

    print()
    print("=" * 70)
    print(f"✓ All streaming queries started ({len(queries)} queries)")
    print("=" * 70)
    print()
    print("Press Ctrl+C to stop...")
    print()

    # Wait for all queries to finish (or Ctrl+C)
    try:
        for query in queries:
            query.awaitTermination()
    except KeyboardInterrupt:
        print("\n\n⚠ Stopping all queries...")
        for query in queries:
            query.stop()
        print("✓ All queries stopped")
        spark.stop()
        print("✓ Spark session closed")


if __name__ == "__main__":
    main()
