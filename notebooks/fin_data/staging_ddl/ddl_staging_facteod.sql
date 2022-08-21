-- Databricks notebook source
DROP TABLE IF EXISTS staging.FactEod;

CREATE TABLE staging.FactEod (
    PK STRING
  , Open FLOAT
  , High FLOAT
  , Low FLOAT
  , Close FLOAT
  , Volume FLOAT
  , SplitFactor FLOAT
  , Dividend FLOAT
  , Symbol STRING
  , ExchangeId STRING
  , `Date` INT
)
USING DELTA
PARTITIONED BY (`Date`)
LOCATION '/mnt/lake/Staging/FactEod'