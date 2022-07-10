-- Databricks notebook source
DROP TABLE IF EXISTS staging.DimCompany;

CREATE TABLE staging.DimCompany (
  PK STRING,
  CompanyId STRING,
  CompanyName STRING,
  ExchangeId STRING
)
USING DELTA
LOCATION '/mnt/lake/Staging/DimCompany'
