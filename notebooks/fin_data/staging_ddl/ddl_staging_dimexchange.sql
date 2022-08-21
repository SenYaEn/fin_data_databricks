-- Databricks notebook source
DROP TABLE IF EXISTS staging.DimExchange;

CREATE TABLE staging.DimExchange (
  ExchangeId STRING,
  Name STRING,
  Acronym STRING,
  Country STRING,
  CountryCode STRING,
  City STRING,
  Website STRING,
  TimezoneName STRING,
  TimezoneCode STRING,
  CurrencyId STRING
)
USING DELTA
LOCATION '/mnt/lake/Staging/DimExchange'