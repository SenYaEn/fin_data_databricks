-- Databricks notebook source
DROP TABLE IF EXISTS staging.FactSplits;

CREATE TABLE staging.FactSplits (
    PK STRING
  , Symbol STRING
  , SplitFactor FLOAT
  , `Date` INT
)
USING DELTA
PARTITIONED BY (`Date`)
LOCATION '/mnt/lake/Staging/FactSplits'