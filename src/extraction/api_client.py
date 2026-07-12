import json
import logging
from datetime import datetime, timezone

import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from src.locations import Location, LOCATIONS

logger = logging.getLogger(__name__)

FORECAST_ENDPOINT = "https://api.open-meteo.com/v1/forecast"

HOURLY_VARIABLES = [
    "temperature_2m",
    "apparent_temperature",
    "relative_humidity_2m",
    "cloud_cover",
    "windspeed_10m",
    "winddirection_10m",
    "precipitation",
    "snowfall",
    "precipitation_probability",
    "rain",
    "weather_code",
    "visibility",
    "is_day",
]

# Bronze table schema — raw JSON stored as-is, transformation happens in Silver.
_BRONZE_SCHEMA = StructType([
    StructField("city",            StringType(),    nullable=False),
    StructField("raw_payload",     StringType(),    nullable=False),
    StructField("ingested_at",     TimestampType(), nullable=False),
    StructField("pipeline_run_id", StringType(),    nullable=False),
])



def fetch_weather(location: Location) -> dict:
    """
    Fetch the 14-day hourly forecast for a single location.
    Returns raw JSON unchanged. Raises on non-200 or API error body.
    """
    params = {
        "latitude": location.latitude,
        "longitude": location.longitude,
        "hourly": HOURLY_VARIABLES,
        "timezone": location.timezone,
        "forecast_days": 14,
    }
    response = requests.get(FORECAST_ENDPOINT, params=params, timeout=30)
    response.raise_for_status()

    payload = response.json()
    if payload.get("error"):
        raise requests.HTTPError(f"API error for {location.city}: {payload.get('reason', 'unknown')}")

    return payload



def ingest_forecast(
    spark: SparkSession,
    table: str,
    pipeline_run_id: str,
    locations: list[Location] = LOCATIONS,
) -> int:
    """
    Fetch the 14-day hourly forecast for all locations and append raw payloads
    to the Bronze Delta table. Intended to run once daily.

    Returns the number of rows successfully written.
    """
    ingested_at = datetime.now(timezone.utc)
    rows = []

    for location in locations:
        try:
            payload = fetch_weather(location)
            rows.append((location.city, json.dumps(payload), ingested_at, pipeline_run_id))
            logger.info("Fetched forecast for %s", location.city)
        except Exception as exc:
            logger.error("Failed to fetch forecast for %s: %s", location.city, exc)

    if not rows:
        logger.warning("No forecast data fetched — nothing written to %s", table)
        return 0

    df = spark.createDataFrame(rows, schema=_BRONZE_SCHEMA)
    df.write.format("delta").mode("append").saveAsTable(table)

    logger.info("Wrote %d forecast rows to %s", len(rows), table)
    return len(rows)


