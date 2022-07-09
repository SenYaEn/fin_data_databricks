-- Databricks notebook source
-- MAGIC %python
-- MAGIC landing_database_name = dbutils.widgets.get('landing_database_name')

-- COMMAND ----------

DROP TABLE IF EXISTS $landing_database_name.FactDividends;

CREATE TABLE $landing_database_name.FactDividends (
   `date` STRING
  , dividend FLOAT
  , symbol STRING
  , pk STRING
  , dayId INT
  , runId INT
)
USING PARQUET
PARTITIONED BY (dayId, runId)
LOCATION '/mnt/lake/Landing/Dividends';

MSCK REPAIR TABLE $landing_database_name.FactDividends;