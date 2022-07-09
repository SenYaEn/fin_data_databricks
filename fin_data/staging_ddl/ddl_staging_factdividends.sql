-- Databricks notebook source
DROP TABLE IF EXISTS staging.FactDividends;

CREATE TABLE staging.FactDividends (
    PK STRING
  , Symbol STRING
  , Dividend FLOAT
  , `Date` INT
)
USING DELTA
PARTITIONED BY (`Date`)
LOCATION '/mnt/lake/Staging/FactDividends'