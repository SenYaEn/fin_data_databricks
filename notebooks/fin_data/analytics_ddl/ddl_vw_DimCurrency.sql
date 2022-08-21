-- Databricks notebook source
DROP VIEW IF EXISTS analytics.vw_DimCurrency;

CREATE VIEW analytics.vw_DimCurrency AS
(
  SELECT CurrencyId
       , Symbol
       , Name
  FROM staging.DimCurrency
)