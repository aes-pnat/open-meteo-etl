# TODO: Implement Silver → Gold star schema transform.
#
# Tables to build (dimensions before fact):
#   dim_location      – one row per city, with surrogate key
#   dim_date          – calendar spine with year/month/week/etc. attributes
#   dim_weather_code  – WMO code → human-readable label + category
#   fact_weather_daily – grain: (location, date); FKs to all three dims
#
# All loads should use MERGE (upsert) so the pipeline is safe to re-run.
# Fact table should include at least one derived measure
# (e.g. avg temp, temp range, daylight hours).
