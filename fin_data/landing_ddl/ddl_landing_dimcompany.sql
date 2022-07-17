-- Databricks notebook source
-- MAGIC %python
-- MAGIC landing_database_name = dbutils.widgets.get('landing_database_name')

-- COMMAND ----------

DROP TABLE IF EXISTS $landing_database_name.DimCompany;

CREATE TABLE $landing_database_name.DimCompany (
  name STRING,
  symbol STRING,
  has_intraday BOOLEAN,
  has_eod BOOLEAN,
  country STRING,
  exchangeMic STRING,
  exchangeAcronym STRING,
  pk STRING,
  dayId INT,
  runId INT
)
USING PARQUET
PARTITIONED BY (dayId, runId)
LOCATION '/mnt/lake/Landing/Companies';

MSCK REPAIR TABLE $landing_database_name.DimCompany;