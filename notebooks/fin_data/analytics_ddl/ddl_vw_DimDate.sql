-- Databricks notebook source
DROP VIEW IF EXISTS analytics.vw_DimDate;

CREATE VIEW analytics.vw_DimDate AS
(
  SELECT DateNum
      , `Date`
      , YearId
      , YearMonthNum
      , Calendar_Quarter
      , MonthNum
      , MonthName
      , MonthShortName
      , WeekNum
      , DayNumOfYear
      , DayNumOfMonth
      , DayNumOfWeek
      , DayName
      , DayShortName
      , `Quarter`
      , YearQuarterNum
      , DayNumOfQuarter
      , CONCAT(YearId, ' Q', `Quarter`) AS YearAndQuarter
  FROM staging.DimDate
)