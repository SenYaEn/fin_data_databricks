-- Databricks notebook source
-- MAGIC %run Users/jan.strocki@hotmail.co.uk/finData/helper

-- COMMAND ----------

DROP TABLE IF EXISTS staging.DimDate;

CREATE TABLE staging.DimDate (
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
USING DELTA
LOCATION '/mnt/lake/Staging/DimDate';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC source_table = 'landing.DimDate'
-- MAGIC destination_table = 'staging.DimDate'
-- MAGIC merge_keys = 'destination.DateNum = updates.DateNum'
-- MAGIC column_mapping = {
-- MAGIC                       "DateNum": "updates.DateNum",
-- MAGIC                       "`Date`": "updates.`Date`",
-- MAGIC                       "YearMonthNum": "updates.YearMonthNum",
-- MAGIC                       "Calendar_Quarter": "updates.Calendar_Quarter",
-- MAGIC                       "MonthNum": "updates.MonthNum",
-- MAGIC                       "MonthName": "updates.MonthName",
-- MAGIC                       "MonthShortName": "updates.MonthShortName",
-- MAGIC                       "WeekNum": "updates.WeekNum",
-- MAGIC                       "DayNumOfYear": "updates.DayNumOfYear",
-- MAGIC                       "DayNumOfMonth": "updates.DayNumOfMonth",
-- MAGIC                       "DayNumOfWeek": "updates.DayNumOfWeek",
-- MAGIC                       "DayName": "updates.DayName",
-- MAGIC                       "DayShortName": "updates.DayShortName",
-- MAGIC                       "`Quarter`": "updates.`Quarter`",
-- MAGIC                       "YearQuarterNum": "updates.YearQuarterNum",
-- MAGIC                       "DayNumOfQuarter": "updates.DayNumOfQuarter"
-- MAGIC                   }
-- MAGIC 
-- MAGIC source_df = spark.sql(f"SELECT * FROM {source_table}")
-- MAGIC 
-- MAGIC destination_directory = get_table_location (destination_table)['full_location'].replace('dbfs:', '')  
-- MAGIC destination_delta_table = DeltaTable.forPath(spark, destination_directory)
-- MAGIC 
-- MAGIC (destination_delta_table.alias('destination')
-- MAGIC  .merge(source_df.alias('updates'), f"{merge_keys}")
-- MAGIC  .whenMatchedUpdate(set = column_mapping)
-- MAGIC  .whenNotMatchedInsert(values = column_mapping)
-- MAGIC  .execute())