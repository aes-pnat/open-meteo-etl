# TODO: Unit tests for Silver and Gold transforms.
# Use PySpark local mode (no cluster needed).
#
# Suggested cases:
#   Silver: JSON parse produces correct row count, correct column types, no nulls on key fields
#   Gold dim_date: date_sk = YYYYMMDD int, is_weekend flag, no gaps in date spine
#   Gold fact: derived measures (avg temp, range, daylight hours), unique fact_sk
