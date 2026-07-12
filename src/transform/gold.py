from pyspark.sql import SparkSession


def load_dim_location(spark: SparkSession) -> None:
    """
    Upsert dim_location from bronze.open_meteo.locations.

    SK assignment: existing rows keep their location_sk (WHEN MATCHED only updates
    attributes). New cities receive MAX(existing_sk) + ROW_NUMBER() so keys are
    stable across re-runs and remain sequential as cities are added.
    """
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW _dim_location_staging AS
        WITH joined AS (
            SELECT
                s.city, s.latitude, s.longitude, s.timezone,
                t.location_sk AS existing_sk
            FROM bronze.open_meteo.locations s
            LEFT JOIN gold.open_meteo.dim_location t ON s.city = t.city
        ),
        new_rows AS (
            SELECT
                city, latitude, longitude, timezone,
                (SELECT COALESCE(MAX(location_sk), 0) FROM gold.open_meteo.dim_location)
                + ROW_NUMBER() OVER (ORDER BY city) AS location_sk
            FROM joined
            WHERE existing_sk IS NULL
        )
        SELECT
            COALESCE(j.existing_sk, n.location_sk) AS location_sk,
            j.city, j.latitude, j.longitude, j.timezone
        FROM joined j
        LEFT JOIN new_rows n ON j.city = n.city
    """)

    spark.sql("""
        MERGE INTO gold.open_meteo.dim_location AS target
        USING _dim_location_staging AS source
          ON target.city = source.city
        WHEN MATCHED THEN UPDATE SET
            target.latitude  = source.latitude,
            target.longitude = source.longitude,
            target.timezone  = source.timezone
        WHEN NOT MATCHED THEN INSERT *
    """)


def load_dim_date(spark: SparkSession) -> None:
    """
    Upsert dim_date from the distinct dates present in silver.open_meteo.weather.
    date_sk is YYYYMMDD as INT — deterministic, self-documenting, no sequence needed.
    """
    spark.sql("""
        MERGE INTO gold.open_meteo.dim_date AS target
        USING (
            SELECT DISTINCT
                CAST(DATE_FORMAT(DATE(forecast_time), 'yyyyMMdd') AS INT) AS date_sk,
                DATE(forecast_time)                                        AS date,
                YEAR(forecast_time)                                        AS year,
                QUARTER(forecast_time)                                     AS quarter,
                MONTH(forecast_time)                                       AS month,
                DATE_FORMAT(forecast_time, 'MMMM')                        AS month_name,
                DAY(forecast_time)                                         AS day_of_month,
                DAYOFWEEK(forecast_time)                                   AS day_of_week,
                DATE_FORMAT(forecast_time, 'EEEE')                        AS day_name,
                WEEKOFYEAR(forecast_time)                                  AS week_of_year,
                DAYOFWEEK(forecast_time) IN (1, 7)                        AS is_weekend
            FROM silver.open_meteo.weather
        ) AS source
        ON target.date_sk = source.date_sk
        WHEN MATCHED     THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def load_dim_weather_code(spark: SparkSession) -> None:
    """
    Upsert dim_weather_code from bronze.open_meteo.weather_codes.
    No surrogate key — the WMO code is a stable integer standard and is used
    directly as the PK and as the FK in the fact table.
    """
    spark.sql("""
        MERGE INTO gold.open_meteo.dim_weather_code AS target
        USING bronze.open_meteo.weather_codes AS source
          ON target.weather_code = source.weather_code
        WHEN MATCHED     THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def load_fact_weather_hourly(spark: SparkSession) -> None:
    """
    Project Silver typed rows into the hourly Gold fact table.
    Joins to dim_location to resolve city → location_sk.
    MERGE key: (location_sk, date_sk, hour_of_day).
    """
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW _fact_weather_hourly_staging AS
        SELECT
            l.location_sk,
            CAST(DATE_FORMAT(DATE(s.forecast_time), 'yyyyMMdd') AS INT) AS date_sk,
            HOUR(s.forecast_time)        AS hour_of_day,
            s.weather_code,
            s.temperature_2m,
            s.apparent_temperature,
            s.relative_humidity_2m,
            s.cloud_cover,
            s.windspeed_10m,
            s.winddirection_10m,
            s.precipitation,
            s.snowfall,
            s.precipitation_probability,
            s.rain,
            s.visibility,
            s.is_day
        FROM silver.open_meteo.weather s
        JOIN gold.open_meteo.dim_location l ON l.city = s.city
    """)

    spark.sql("""
        MERGE INTO gold.open_meteo.fact_weather_hourly AS target
        USING _fact_weather_hourly_staging AS source
          ON  target.location_sk = source.location_sk
         AND  target.date_sk     = source.date_sk
         AND  target.hour_of_day = source.hour_of_day
        WHEN MATCHED     THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def load_fact_weather_daily(spark: SparkSession) -> None:
    """
    Aggregate fact_weather_hourly to daily grain and MERGE into fact_weather_daily.
    Source is the Gold hourly fact — not Silver — so both fact tables are always
    consistent with each other.

    Derived measures:
      temperature_range  = max_temperature_2m - min_temperature_2m
      daylight_hours     = SUM(is_day)  [is_day is 1 per daytime hour, 0 at night]

    weather_code uses MODE() — most frequent hourly code for the day.
    avg_winddirection_10m is an arithmetic mean; a circular mean would be more
    accurate for compass degrees but is sufficient for reporting purposes.
    """
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW _fact_weather_daily_staging AS
        SELECT
            location_sk,
            date_sk,
            MODE(weather_code)                                         AS weather_code,
            ROUND(AVG(temperature_2m),            2) AS avg_temperature_2m,
            ROUND(MIN(temperature_2m),            2) AS min_temperature_2m,
            ROUND(MAX(temperature_2m),            2) AS max_temperature_2m,
            ROUND(MAX(temperature_2m) - MIN(temperature_2m), 2) AS temperature_range,
            ROUND(AVG(apparent_temperature),      2) AS avg_apparent_temperature,
            ROUND(AVG(relative_humidity_2m),      2) AS avg_relative_humidity_2m,
            ROUND(AVG(cloud_cover),               2) AS avg_cloud_cover,
            ROUND(AVG(windspeed_10m),             2) AS avg_windspeed_10m,
            ROUND(AVG(winddirection_10m),         2) AS avg_winddirection_10m,
            ROUND(SUM(precipitation),             2) AS total_precipitation,
            ROUND(SUM(snowfall),                  2) AS total_snowfall,
            ROUND(SUM(rain),                      2) AS total_rain,
            ROUND(AVG(precipitation_probability), 2) AS avg_precipitation_probability,
            ROUND(AVG(visibility),                2) AS avg_visibility,
            CAST(SUM(is_day) AS DOUBLE)               AS daylight_hours
        FROM gold.open_meteo.fact_weather_hourly
        GROUP BY location_sk, date_sk
    """)

    spark.sql("""
        MERGE INTO gold.open_meteo.fact_weather_daily AS target
        USING _fact_weather_daily_staging AS source
          ON  target.location_sk = source.location_sk
         AND  target.date_sk     = source.date_sk
        WHEN MATCHED     THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def transform_silver_to_gold(spark: SparkSession) -> tuple[int, int]:
    """
    Load all Gold tables in dependency order: dimensions first, hourly fact second,
    daily fact last (derived from hourly).
    Returns (hourly_row_count, daily_row_count).
    """
    load_dim_location(spark)
    load_dim_date(spark)
    load_dim_weather_code(spark)
    load_fact_weather_hourly(spark)
    load_fact_weather_daily(spark)
    hourly = spark.sql("SELECT COUNT(*) FROM gold.open_meteo.fact_weather_hourly").collect()[0][0]
    daily  = spark.sql("SELECT COUNT(*) FROM gold.open_meteo.fact_weather_daily").collect()[0][0]
    return hourly, daily

