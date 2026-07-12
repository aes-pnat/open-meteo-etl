# Databricks notebook source

# MAGIC %md
# MAGIC # Silver — Transform & Load
# MAGIC Parses raw JSON from `bronze.open_meteo.raw_weather` into typed hourly rows
# MAGIC in `silver.open_meteo.weather`. One row per `(city, forecast_time)`.
# MAGIC Safe to re-run — Bronze is deduplicated on `row_hash` before processing
# MAGIC and the load uses MERGE on `(city, forecast_time)`.

# COMMAND ----------
# MAGIC %md ## 1. Create schema and table

# COMMAND ----------

from src.transform.silver import transform_bronze_to_silver

spark.sql("CREATE CATALOG IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver.open_meteo")

spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.open_meteo.weather (
        city                      STRING     NOT NULL  COMMENT 'Location name',
        forecast_time             TIMESTAMP  NOT NULL  COMMENT 'Hourly forecast timestamp (location-local, stored as-is)',
        temperature_2m            DOUBLE               COMMENT 'Air temperature at 2 m (°C)',
        apparent_temperature      DOUBLE               COMMENT 'Apparent (feels-like) temperature (°C)',
        relative_humidity_2m      INT                  COMMENT 'Relative humidity at 2 m (%)',
        cloud_cover               INT                  COMMENT 'Total cloud cover (%)',
        windspeed_10m             DOUBLE               COMMENT 'Wind speed at 10 m (km/h)',
        winddirection_10m         INT                  COMMENT 'Wind direction at 10 m (°)',
        precipitation             DOUBLE               COMMENT 'Total precipitation (mm)',
        snowfall                  DOUBLE               COMMENT 'Snowfall (cm)',
        precipitation_probability INT                  COMMENT 'Precipitation probability (%)',
        rain                      DOUBLE               COMMENT 'Rain (mm)',
        weather_code              INT                  COMMENT 'WMO weather interpretation code',
        visibility                DOUBLE               COMMENT 'Visibility (m)',
        is_day                    INT                  COMMENT '1 = daytime, 0 = night',
        ingested_at               TIMESTAMP  NOT NULL  COMMENT 'UTC ingestion timestamp from the Bronze row',
        pipeline_run_id           STRING     NOT NULL  COMMENT 'UUID of the pipeline run that produced this row'
    )
    USING DELTA
    COMMENT 'Typed hourly forecast — Silver layer; grain: (city, forecast_time)'
""")

# COMMAND ----------
# MAGIC %md ## 2. Run transform

# COMMAND ----------

total_rows = transform_bronze_to_silver(spark)
print(f"Silver total rows: {total_rows:,}  (expect {23 * 336:,} per full 14-day run)")

# COMMAND ----------
# MAGIC %md ## 3. Validation

# COMMAND ----------

# Row count per city — all 23 should have 336 rows (14 days × 24 h)
display(spark.sql("""
    SELECT   city,
             COUNT(*)                          AS hour_count,
             MIN(forecast_time)                AS earliest,
             MAX(forecast_time)                AS latest
    FROM     silver.open_meteo.weather
    GROUP BY city
    ORDER BY city
"""))

# COMMAND ----------

# Spot nulls in key columns — should all be zero. In production, we would monitor multiple columns for nulls and faulty types, extracting the results to a Delta table for alerting and dashboarding. For this demo, we just print the counts.
display(spark.sql("""
    SELECT
        SUM(CASE WHEN forecast_time    IS NULL THEN 1 ELSE 0 END) AS null_forecast_time,
        SUM(CASE WHEN temperature_2m   IS NULL THEN 1 ELSE 0 END) AS null_temperature,
        SUM(CASE WHEN weather_code     IS NULL THEN 1 ELSE 0 END) AS null_weather_code
    FROM silver.open_meteo.weather
"""))
