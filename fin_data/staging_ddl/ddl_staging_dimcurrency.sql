-- Databricks notebook source
DROP TABLE IF EXISTS staging.DimCurrency;

CREATE TABLE staging.DimCurrency (
  CurrencyId STRING,
  Symbol STRING,
  Name STRING
)
USING DELTA
LOCATION '/mnt/lake/Staging/DimCurrency'