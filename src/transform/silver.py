from pyspark.sql import SparkSession

# DDL schema string passed to from_json — covers only the 'hourly' block.
# Extra top-level keys in the payload (latitude, elevation, …) are silently ignored.
_PAYLOAD_SCHEMA_DDL = (
    "struct<hourly:struct<"
    "time:array<string>,"
    "temperature_2m:array<double>,"
    "apparent_temperature:array<double>,"
    "relative_humidity_2m:array<int>,"
    "cloud_cover:array<int>,"
    "windspeed_10m:array<double>,"
    "winddirection_10m:array<int>,"
    "precipitation:array<double>,"
    "snowfall:array<double>,"
    "precipitation_probability:array<int>,"
    "rain:array<double>,"
    "weather_code:array<int>,"
    "visibility:array<double>,"
    "is_day:array<int>"
    ">>"
)


def transform_bronze_to_silver(spark: SparkSession) -> int:
    """
    Parse Bronze JSON payloads into typed hourly rows and MERGE into
    silver.open_meteo.weather on (city, forecast_time).
    Returns total row count after the merge.
    """
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW _silver_staging AS
        WITH bronze_parsed AS (
            SELECT
                city,
                ingested_at,
                pipeline_run_id,
                from_json(raw_payload, '{_PAYLOAD_SCHEMA_DDL}').hourly AS hourly
            FROM bronze.open_meteo.raw_weather
            QUALIFY ROW_NUMBER() OVER (PARTITION BY row_hash ORDER BY ingested_at DESC) = 1
        ),
        bronze_arrays AS (
            SELECT
                city,
                ingested_at,
                pipeline_run_id,
                hourly.time                       AS time,
                hourly.temperature_2m             AS temperature_2m,
                hourly.apparent_temperature       AS apparent_temperature,
                hourly.relative_humidity_2m       AS relative_humidity_2m,
                hourly.cloud_cover                AS cloud_cover,
                hourly.windspeed_10m              AS windspeed_10m,
                hourly.winddirection_10m          AS winddirection_10m,
                hourly.precipitation              AS precipitation,
                hourly.snowfall                   AS snowfall,
                hourly.precipitation_probability  AS precipitation_probability,
                hourly.rain                       AS rain,
                hourly.weather_code               AS weather_code,
                hourly.visibility                 AS visibility,
                hourly.is_day                     AS is_day
            FROM bronze_parsed
        ),
        exploded AS (
            SELECT
                city,
                ingested_at,
                pipeline_run_id,
                explode(arrays_zip(
                    time, temperature_2m, apparent_temperature, relative_humidity_2m,
                    cloud_cover, windspeed_10m, winddirection_10m, precipitation,
                    snowfall, precipitation_probability, rain, weather_code,
                    visibility, is_day
                )) AS h
            FROM bronze_arrays
        )
        SELECT
            city,
            to_timestamp(h.time, "yyyy-MM-dd'T'HH:mm")  AS forecast_time,
            h.temperature_2m,
            h.apparent_temperature,
            h.relative_humidity_2m,
            h.cloud_cover,
            h.windspeed_10m,
            h.winddirection_10m,
            h.precipitation,
            h.snowfall,
            h.precipitation_probability,
            h.rain,
            h.weather_code,
            h.visibility,
            h.is_day,
            ingested_at,
            pipeline_run_id
        FROM exploded
    """)

    spark.sql("""
        MERGE INTO silver.open_meteo.weather AS target
        USING _silver_staging AS source
          ON  target.city          = source.city
         AND  target.forecast_time = source.forecast_time
        WHEN MATCHED     THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    return spark.sql("SELECT COUNT(*) FROM silver.open_meteo.weather").collect()[0][0]


