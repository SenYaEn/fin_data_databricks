-- Databricks notebook source
-- MAGIC %python
-- MAGIC landing_database_name = dbutils.widgets.get('landing_database_name')

-- COMMAND ----------

DROP TABLE IF EXISTS $landing_database_name.FactEod;

CREATE TABLE $landing_database_name.FactEod (
    open FLOAT
  , high FLOAT
  , low FLOAT
  , close FLOAT
  , volume FLOAT
  , adj_high STRING
  , adj_low STRING
  , adj_close FLOAT
  , adj_open STRING
  , adj_volume STRING
  , split_factor FLOAT
  , dividend FLOAT
  , symbol STRING
  , `exchange` STRING
  , `date` INT
  , pk STRING
  , dayId INT
  , runId INT
)
USING PARQUET
PARTITIONED BY (dayId, runId)
LOCATION '/mnt/lake/Landing/Eod';

MSCK REPAIR TABLE $landing_database_name.FactEod;