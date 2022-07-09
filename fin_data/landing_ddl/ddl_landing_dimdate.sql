-- Databricks notebook source
-- MAGIC %python
-- MAGIC landing_database_name = dbutils.widgets.get('landing_database_name')

-- COMMAND ----------

DROP TABLE IF EXISTS $landing_database_name.DimDate;

CREATE TABLE $landing_database_name.DimDate (
    DateNum INT
  , `Date` DATE
  , YearMonthNum INT
  , Calendar_Quarter STRING
  , MonthNum INT
  , MonthName STRING
  , MonthShortName STRING
  , WeekNum INT
  , DayNumOfYear INT
  , DayNumOfMonth INT
  , DayNumOfWeek INT
  , DayName STRING
  , DayShortName STRING
  , `Quarter` INT
  , YearQuarterNum INT
  , DayNumOfQuarter INT
)
USING CSV
OPTIONS (header 'true', delimiter '|')
LOCATION '/mnt/lake/Landing/Date';

-- COMMAND ----------

select * from $landing_database_name.DimDate