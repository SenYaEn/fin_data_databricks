-- Databricks notebook source
DROP VIEW IF EXISTS analytics.vw_DimCompanyExchange;

CREATE VIEW analytics.vw_DimCompanyExchange AS
(
  SELECT PK AS CompanyExchangeId
       , CompanyId
       , CompanyName
       , ExchangeId
    FROM staging.DimCompany
   WHERE CompanyName IS NOT NULL
)