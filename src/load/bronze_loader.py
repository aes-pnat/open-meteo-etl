# TODO: Implement Bronze layer loader.
#
# create_bronze_table_if_not_exists(spark)
#   - DDL for the raw landing table (raw_payload STRING, ingested_at TIMESTAMP, etc.)
#
# write_raw_payloads(spark, payloads)
#   - Convert (location, raw_json) pairs to a DataFrame
#   - Append to the Bronze Delta table (append-only, no upsert)
