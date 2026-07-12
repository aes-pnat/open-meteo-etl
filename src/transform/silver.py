# TODO: Implement Bronze → Silver transform.
#
# create_silver_table_if_not_exists(spark)
#   - DDL for the typed Silver table (one row per location+date)
#
# transform_bronze_to_silver(spark)
#   - Read raw_payload from Bronze
#   - Parse JSON with from_json, explode parallel daily arrays into rows
#   - Cast to correct types, rename columns
#   - MERGE into Silver on (source_location, date) – no duplicate rows on re-run
