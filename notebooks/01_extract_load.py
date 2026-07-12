# Databricks notebook source

# MAGIC %md
# MAGIC # Bronze — Extract & Load
# MAGIC Runs daily. Fetches the 14-day hourly forecast from Open-Meteo for all
# MAGIC 23 locations and lands raw JSON in `bronze.open_meteo.raw_weather`.
# MAGIC Location seed data is upserted into `bronze.open_meteo.locations`.
# MAGIC WMO weather code reference data is upserted into `bronze.open_meteo.weather_codes`.

# COMMAND ----------

import uuid
from datetime import datetime, timezone

from src.extraction.api_client import ingest_forecast
from src.locations import LOCATIONS
from src.weather_codes import WEATHER_CODES

# Single UUID ties every row written in this execution together for lineage.
pipeline_run_id = str(uuid.uuid4())
print(f"pipeline_run_id: {pipeline_run_id}")

# COMMAND ----------
# MAGIC %md ## 1. Create schemas and tables

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze.open_meteo")

spark.sql("""
    CREATE TABLE IF NOT EXISTS bronze.open_meteo.raw_weather (
        city             STRING    NOT NULL  COMMENT 'Location name from LOCATIONS',
        raw_payload      STRING    NOT NULL  COMMENT 'Full API response JSON, unparsed',
        ingested_at      TIMESTAMP NOT NULL  COMMENT 'UTC timestamp of the API call',
        pipeline_run_id  STRING    NOT NULL  COMMENT 'UUID linking all rows from one run',
        row_hash         BIGINT    GENERATED ALWAYS AS (xxhash64(raw_payload))
                                             COMMENT 'xxHash64 of raw_payload — use for deduplication'
    )
    USING DELTA
    COMMENT 'Raw Open-Meteo 14-day hourly forecast JSON — Bronze landing layer'
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS bronze.open_meteo.locations (
        city             STRING    NOT NULL,
        latitude         DOUBLE    NOT NULL,
        longitude        DOUBLE    NOT NULL,
        timezone         STRING    NOT NULL,
        ingested_at      TIMESTAMP NOT NULL  COMMENT 'UTC timestamp of last upsert',
        pipeline_run_id  STRING    NOT NULL
    )
    USING DELTA
    COMMENT 'Location reference seed — sourced from src/locations.py'
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS bronze.open_meteo.weather_codes (
        weather_code  INT     NOT NULL  COMMENT 'WMO weather interpretation code',
        description   STRING  NOT NULL  COMMENT 'Human-readable weather description'
    )
    USING DELTA
    COMMENT 'WMO weather code reference — static seed data; sourced from src/weather_codes.py'
""")

# COMMAND ----------
# MAGIC %md ## 2. Upsert location seed data

# COMMAND ----------

ingested_at = datetime.now(timezone.utc)

location_rows = [
    (loc.city, loc.latitude, loc.longitude, loc.timezone, ingested_at, pipeline_run_id)
    for loc in LOCATIONS
]

(
    spark
    .createDataFrame(location_rows, schema=["city", "latitude", "longitude", "timezone", "ingested_at", "pipeline_run_id"])
    .createOrReplaceTempView("_location_staging")
)

spark.sql("""
    MERGE INTO bronze.open_meteo.locations AS target
    USING _location_staging AS source
      ON target.city = source.city
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"Locations upserted: {len(location_rows)}")

# COMMAND ----------
# MAGIC %md ## 3. Upsert weather code reference data

# COMMAND ----------

weather_code_rows = [(wc.code, wc.description) for wc in WEATHER_CODES]

(
    spark
    .createDataFrame(weather_code_rows, schema=["weather_code", "description"])
    .createOrReplaceTempView("_weather_code_staging")
)

spark.sql("""
    MERGE INTO bronze.open_meteo.weather_codes AS target
    USING _weather_code_staging AS source
      ON target.weather_code = source.weather_code
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"Weather codes upserted: {len(weather_code_rows)}")

# COMMAND ----------
# MAGIC %md ## 4. Ingest forecast data

# COMMAND ----------

rows_written = ingest_forecast(spark, "bronze.open_meteo.raw_weather", pipeline_run_id)
print(f"Forecast rows written: {rows_written} / {len(LOCATIONS)} locations")
# Each row = one raw JSON blob for one city. A count below 23 means
# one or more API calls failed and were skipped — check logs above.
if rows_written < len(LOCATIONS):
    raise Exception(
        f"Partial ingest: only {rows_written}/{len(LOCATIONS)} locations succeeded. "
        "Check logs for individual failures."
    )

# COMMAND ----------
# MAGIC %md ## 5. Validation

# COMMAND ----------

# Confirm all locations landed in this run
display(spark.sql(f"""
    SELECT   city, ingested_at, pipeline_run_id
    FROM     bronze.open_meteo.raw_weather
    WHERE    pipeline_run_id = '{pipeline_run_id}'
    ORDER BY city
"""))

# COMMAND ----------

# Row count trend across runs — spot unexpected gaps or duplicates
display(spark.sql("""
    SELECT   DATE(ingested_at) AS run_date,
             COUNT(*)          AS location_count
    FROM     bronze.open_meteo.raw_weather
    GROUP BY DATE(ingested_at)
    ORDER BY run_date DESC
    LIMIT    30
"""))
