# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze — Extract & Load
# MAGIC Runs daily. Fetches the 14-day hourly forecast from Open-Meteo for all
# MAGIC 23 locations and lands raw JSON in `bronze.open_meteo.raw_weather`.
# MAGIC Location seed data is upserted into `bronze.open_meteo.locations`.

# COMMAND ----------
# MAGIC %pip install requests

# COMMAND ----------

import uuid
from datetime import datetime, timezone

from src.extraction.api_client import ingest_forecast
from src.locations import LOCATIONS

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
        pipeline_run_id  STRING    NOT NULL  COMMENT 'UUID linking all rows from one run'
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
# MAGIC %md ## 3. Ingest forecast data

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
# MAGIC %md ## 4. Validation

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
