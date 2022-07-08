-- Databricks notebook source
-- MAGIC %python
-- MAGIC landing_database_name = dbutils.widgets.get('landing_database_name')

-- COMMAND ----------

DROP TABLE IF EXISTS $landing_database_name.DimExchange;

CREATE TABLE $landing_database_name.DimExchange (
  name STRING,
  acronym STRING,
  mic STRING,
  country STRING,
  country_code STRING,
  city STRING,
  website STRING,   
  currencyCode STRING,
  timezoneName STRING,
  timezoneAbbr STRING,
  timezoneAbbrDst STRING,
  dayId INT,
  runId INT
)
USING PARQUET
PARTITIONED BY (dayId, runId)
LOCATION '/mnt/lake/Landing/Exchanges';

MSCK REPAIR TABLE $landing_database_name.DimExchange;