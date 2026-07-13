# open-meteo-etl

Weather data pipeline that pulls hourly forecasts from the Open-Meteo API for 23 corporate locations, processes them through a medallion architecture on Databricks, and outputs a star schema surfaced in a Databricks Lakeview dashboard.

> **Workspace access:** To explore the live Databricks workspace or browse the Delta tables directly, please reach out and I will provide access credentials.

## Dashboard

[View the live Lakeview dashboard](https://dbc-bcdd869f-1671.cloud.databricks.com/dashboardsv3/01f17e4a5d361c5987baeb0d5728125e/published?o=7474651388183305) *(requires a Databricks account — contact me for access)*

---

## What it does

1. **Extracts** 14-day hourly weather forecasts from the free [Open-Meteo API](https://open-meteo.com/en/docs) for all 23 locations.
2. **Lands** the raw API responses as-is into a Bronze Delta table — nothing is parsed or transformed at this stage.
3. **Transforms** the raw JSON into clean, typed, one-row-per-hour records in Silver.
4. **Aggregates** the Silver data into a star schema in Gold, with both hourly and daily fact tables and three dimension tables.
5. **Schedules** the full pipeline as a Databricks job that runs on demand or on a daily cron.

---

## Architecture

```
Open-Meteo API
      │
      ▼
 Bronze layer          Raw JSON blobs, one row per city per run
 (bronze.open_meteo)   Append-only, never transformed in place
      │
      ▼
 Silver layer          Parsed, typed hourly rows (one row per city × hour)
 (silver.open_meteo)   JSON exploded into columns, MERGE for idempotency
      │
      ▼
 Gold layer            Star schema — dimensions + hourly & daily fact tables
 (gold.open_meteo)     Ready for Power BI
```

All layers are Delta tables in Databricks Unity Catalog.

---

## Repo structure

```
notebooks/
  01_extract_load.py      Bronze — creates tables, upserts location/weather-code
                          seeds, calls the API, writes raw JSON
  02_transform_silver.py  Silver — parses JSON, explodes hourly arrays, MERGEs
  03_transform_gold.py    Gold — builds star schema from Silver

src/
  locations.py            The 23 museum locations (city, lat, lon, timezone)
  weather_codes.py        WMO weather code → description lookup table
  extraction/
    api_client.py         HTTP fetch logic and Bronze write (fetch_weather,
                          ingest_forecast)
  transform/
    silver.py             Bronze → Silver SQL transform (transform_bronze_to_silver)
    gold.py               Silver → Gold SQL transforms (one function per table)

.github/workflows/
  deploy.yml              GitHub Actions — runs `databricks bundle deploy` on
                          every push to main, keeping the Databricks job in sync

databricks.yml            Databricks Asset Bundle config — defines the job,
                          task dependencies, and git_source so notebooks are
                          always pulled fresh from GitHub

requirements.txt          Python dependencies
temp/
  powerbi_report_plan.md  Report specification (visuals, DAX measures, layout)
```

---

## Gold star schema

The final output in `gold.open_meteo`:

```
dim_location        city, lat, lon, timezone — surrogate key location_sk
dim_date            calendar spine (year/month/week/is_weekend) — date_sk = YYYYMMDD int
dim_weather_code    WMO code → description — natural key, no surrogate needed

fact_weather_hourly grain: (location_sk, date_sk, hour_of_day)
                    all raw measures: temperature, humidity, wind, precip, visibility…

fact_weather_daily  grain: (location_sk, date_sk)
                    aggregated from fact_weather_hourly:
                    avg/min/max temperature, temperature_range (derived),
                    total precipitation, daylight_hours (derived), and more
```

All tables use MERGE on their natural keys so the pipeline is safe to re-run.

---

## Technology choices

| What                             | Why                                                                                                                               |
| -------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| **Databricks + Delta Lake**      | Familiar environment; Unity Catalog gives clean catalog/schema/table hierarchy; Delta's MERGE handles idempotent upserts natively |
| **Medallion architecture**       | Separates raw landing (Bronze) from cleaning (Silver) from modelling (Gold) — each layer is independently queryable and reusable  |
| **Spark SQL over DataFrame API** | Transformations are expressed as plain SQL CTEs, making them readable without PySpark knowledge                                   |
| **Open-Meteo**                   | Free, no auth required, reliable hourly forecast data with a clean JSON structure                                                 |
| **Databricks Asset Bundles**     | Version-controlled job definition; `git_source` means notebooks are always fetched from the latest GitHub commit — no manual sync |
| **GitHub Actions**               | Keeps the Databricks job definition up to date automatically on every push                                                        |

---

## Running locally (development)

```bash
pip install -r requirements.txt

# Quick API smoke test — prints payload shape for one location
python tests/try_api.py
```

---

## Deploying to Databricks

Prerequisites: [Databricks CLI v0.200+](https://docs.databricks.com/dev-tools/cli/install.html) installed and authenticated (`databricks configure --token`).

```bash
# Deploy (creates/updates the job in your workspace)
databricks bundle deploy

# Run the full pipeline manually
databricks bundle run open_meteo_etl_job
```

The GitHub Actions workflow in `.github/workflows/deploy.yml` does this automatically on every push to `main`.
