# Databricks notebook source
# TODO: Transform Silver → Gold (Star Schema)
# 1. Create Gold tables if not exist (dim_location, dim_date, dim_weather_code, fact_weather_daily)
# 2. Load dimensions (MERGE on natural key)
# 3. Load fact table last (MERGE on location_sk + date_sk)
# 4. Smoke-test with a joined query across all four tables
