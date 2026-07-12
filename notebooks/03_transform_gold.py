# Databricks notebook source

# MAGIC %md
# MAGIC # Gold — Star Schema Transform & Load
# MAGIC Aggregates Silver hourly rows into a daily star schema in `gold.open_meteo`.
# MAGIC
# MAGIC Load order (dimensions before fact):
# MAGIC 1. `dim_location`       — one row per city; surrogate key assigned on first insert
# MAGIC 2. `dim_date`           — calendar spine; `date_sk` = YYYYMMDD integer
# MAGIC 3. `dim_weather_code`   — WMO reference; natural key used as PK (no SK needed)
# MAGIC 4. `fact_weather_hourly` — grain: `(location_sk, date_sk, hour_of_day)`; sourced from Silver
# MAGIC 5. `fact_weather_daily`  — grain: `(location_sk, date_sk)`; aggregated from `fact_weather_hourly`

# COMMAND ----------
# MAGIC %md ## 1. Create catalog, schema, and tables

# COMMAND ----------

from src.transform.gold import transform_silver_to_gold

spark.sql("CREATE CATALOG IF NOT EXISTS gold")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold.open_meteo")

spark.sql("""
    CREATE TABLE IF NOT EXISTS gold.open_meteo.dim_location (
        location_sk  INT     NOT NULL  COMMENT 'Surrogate key',
        city         STRING  NOT NULL  COMMENT 'Natural key — city name',
        latitude     DOUBLE  NOT NULL,
        longitude    DOUBLE  NOT NULL,
        timezone     STRING  NOT NULL  COMMENT 'IANA timezone'
    )
    USING DELTA
    COMMENT 'Location dimension — one row per museum city'
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS gold.open_meteo.dim_date (
        date_sk      INT     NOT NULL  COMMENT 'Surrogate key — YYYYMMDD integer (e.g. 20260712)',
        date         DATE    NOT NULL  COMMENT 'Natural key',
        year         INT     NOT NULL,
        quarter      INT     NOT NULL,
        month        INT     NOT NULL,
        month_name   STRING  NOT NULL,
        day_of_month INT     NOT NULL,
        day_of_week  INT     NOT NULL  COMMENT '1 = Sunday … 7 = Saturday',
        day_name     STRING  NOT NULL,
        week_of_year INT     NOT NULL,
        is_weekend   BOOLEAN NOT NULL
    )
    USING DELTA
    COMMENT 'Date dimension — calendar spine derived from Silver forecast dates'
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS gold.open_meteo.dim_weather_code (
        weather_code  INT    NOT NULL  COMMENT 'Natural key used as PK — WMO code is a stable integer standard; no surrogate key needed',
        description   STRING NOT NULL
    )
    USING DELTA
    COMMENT 'WMO weather code reference — natural key is the PK'
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS gold.open_meteo.fact_weather_hourly (
        location_sk                   INT     NOT NULL  COMMENT 'FK → dim_location.location_sk',
        date_sk                       INT     NOT NULL  COMMENT 'FK → dim_date.date_sk',
        hour_of_day                   INT     NOT NULL  COMMENT 'Hour of day (0–23)',
        weather_code                  INT     NOT NULL  COMMENT 'FK → dim_weather_code.weather_code',
        temperature_2m                DOUBLE            COMMENT 'Air temperature at 2 m (°C)',
        apparent_temperature          DOUBLE            COMMENT 'Apparent (feels-like) temperature (°C)',
        relative_humidity_2m          INT               COMMENT 'Relative humidity at 2 m (%)',
        cloud_cover                   INT               COMMENT 'Total cloud cover (%)',
        windspeed_10m                 DOUBLE            COMMENT 'Wind speed at 10 m (km/h)',
        winddirection_10m             INT               COMMENT 'Wind direction at 10 m (°)',
        precipitation                 DOUBLE            COMMENT 'Precipitation (mm)',
        snowfall                      DOUBLE            COMMENT 'Snowfall (cm)',
        precipitation_probability     INT               COMMENT 'Precipitation probability (%)',
        rain                          DOUBLE            COMMENT 'Rain (mm)',
        visibility                    DOUBLE            COMMENT 'Visibility (m)',
        is_day                        INT               COMMENT '1 = daytime, 0 = night'
    )
    USING DELTA
    COMMENT 'Hourly weather fact — grain: (location_sk, date_sk, hour_of_day); sourced from Silver'
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS gold.open_meteo.fact_weather_daily (
        location_sk                   INT     NOT NULL  COMMENT 'FK → dim_location.location_sk',
        date_sk                       INT     NOT NULL  COMMENT 'FK → dim_date.date_sk',
        weather_code                  INT     NOT NULL  COMMENT 'FK → dim_weather_code.weather_code (mode of hourly codes)',
        avg_temperature_2m            DOUBLE            COMMENT 'Mean hourly temperature (°C)',
        min_temperature_2m            DOUBLE            COMMENT 'Minimum hourly temperature (°C)',
        max_temperature_2m            DOUBLE            COMMENT 'Maximum hourly temperature (°C)',
        temperature_range             DOUBLE            COMMENT 'Derived: max − min temperature (°C)',
        avg_apparent_temperature      DOUBLE            COMMENT 'Mean feels-like temperature (°C)',
        avg_relative_humidity_2m      DOUBLE            COMMENT 'Mean relative humidity (%)',
        avg_cloud_cover               DOUBLE            COMMENT 'Mean cloud cover (%)',
        avg_windspeed_10m             DOUBLE            COMMENT 'Mean wind speed (km/h)',
        avg_winddirection_10m         DOUBLE            COMMENT 'Mean wind direction (°) — arithmetic mean',
        total_precipitation           DOUBLE            COMMENT 'Total precipitation (mm)',
        total_snowfall                DOUBLE            COMMENT 'Total snowfall (cm)',
        total_rain                    DOUBLE            COMMENT 'Total rain (mm)',
        avg_precipitation_probability DOUBLE            COMMENT 'Mean precipitation probability (%)',
        avg_visibility                DOUBLE            COMMENT 'Mean visibility (m)',
        daylight_hours                DOUBLE            COMMENT 'Derived: sum of is_day = count of daytime hours'
    )
    USING DELTA
    COMMENT 'Daily weather fact — grain: (location_sk, date_sk); aggregated from Silver hourly rows'
""")

# COMMAND ----------
# MAGIC %md ## 2. Load dimensions and fact

# COMMAND ----------

hourly_rows, daily_rows = transform_silver_to_gold(spark)
print(f"Hourly fact rows: {hourly_rows:,}  (expect {23 * 14 * 24:,} per full 14-day run)")
print(f"Daily  fact rows: {daily_rows:,}  (expect {23 * 14:,} per full 14-day run)")

# COMMAND ----------
# MAGIC %md ## 3. Smoke test — joined query across all four tables

# COMMAND ----------

display(spark.sql("""
    SELECT
        l.city,
        d.date,
        d.day_name,
        w.description                   AS weather,
        f.avg_temperature_2m,
        f.min_temperature_2m,
        f.max_temperature_2m,
        f.temperature_range,
        f.total_precipitation,
        f.daylight_hours
    FROM     gold.open_meteo.fact_weather_daily f
    JOIN     gold.open_meteo.dim_location     l ON l.location_sk = f.location_sk
    JOIN     gold.open_meteo.dim_date         d ON d.date_sk      = f.date_sk
    JOIN     gold.open_meteo.dim_weather_code w ON w.weather_code = f.weather_code
    ORDER BY l.city, d.date
    LIMIT    100
"""))

# COMMAND ----------

# Validate: every city should have exactly 14 rows in daily and 336 in hourly
display(spark.sql("""
    SELECT
        l.city,
        COUNT(DISTINCT h.date_sk * 100 + h.hour_of_day) AS hourly_rows,
        COUNT(DISTINCT d.date_sk)                        AS daily_rows
    FROM     gold.open_meteo.fact_weather_hourly h
    JOIN     gold.open_meteo.fact_weather_daily  d ON d.location_sk = h.location_sk AND d.date_sk = h.date_sk
    JOIN     gold.open_meteo.dim_location        l ON l.location_sk = h.location_sk
    GROUP BY l.city
    ORDER BY l.city
"""))

