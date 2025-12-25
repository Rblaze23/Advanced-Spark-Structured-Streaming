"""
This file defines the expected schema of the streaming JSON events.

You will use this schema in streaming_app.py to:
- Parse raw JSON messages
- Enable event-time processing
- Detect malformed or incomplete records
"""

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Define the expected schema for valid events
event_schema = StructType([
    StructField("device_id", StringType(), nullable=False),
    StructField("event_time", TimestampType(), nullable=False),
    StructField("temperature", DoubleType(), nullable=True),
    StructField("country", StringType(), nullable=True)
])

# Valid country codes (for data quality checks)
VALID_COUNTRIES = {"FR", "USA", "france", "France", "Germany", "Germnay", "United States"}

# Temperature thresholds for validation
MIN_VALID_TEMPERATURE = -50.0
MAX_VALID_TEMPERATURE = 60.0
INVALID_TEMPERATURE_MARKER = -999.0
