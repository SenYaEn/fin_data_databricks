-- Databricks notebook source
DROP VIEW IF EXISTS analytics.vw_DimExchange;

CREATE VIEW analytics.vw_DimExchange AS
(
  SELECT ExchangeId
       , Name
       , Acronym
       , Country
       , CountryCode
       , City
       , Website
       , TimezoneName
       , TimezoneCode
       , CurrencyId
  FROM staging.DimExchange
)