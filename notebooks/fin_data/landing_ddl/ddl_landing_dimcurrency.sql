-- Databricks notebook source
-- MAGIC %python
-- MAGIC landing_database_name = dbutils.widgets.get('landing_database_name')

-- COMMAND ----------

DROP TABLE IF EXISTS $landing_database_name.DimCurrency;

CREATE TABLE $landing_database_name.DimCurrency (
  `code` STRING,
  symbol STRING,
  name STRING,
  dayId INT,
  runId INT
)
USING PARQUET
PARTITIONED BY (dayId, runId)
LOCATION '/mnt/lake/Landing/Currencies';

MSCK REPAIR TABLE $landing_database_name.DimCurrency;