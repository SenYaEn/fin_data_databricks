-- Databricks notebook source
DROP VIEW IF EXISTS analytics.vw_DimCompany;

CREATE VIEW analytics.vw_DimCompany AS
(
  WITH cte AS 
  (
    SELECT c.CompanyId
         , c.CompanyName
         , c.ExchangeId 
         , e.Country
         , ROW_NUMBER() OVER (PARTITION BY c.CompanyId ORDER BY c.ExchangeId DESC) AS ROWNUM 
    FROM staging.DimCompany AS c
    INNER JOIN staging.DimExchange AS e
    ON c.ExchangeId = e.ExchangeId
  )
  SELECT CompanyId
       , CompanyName
       , ExchangeId
       , Country
    FROM cte
   WHERE ROWNUM = 1
   AND CompanyName IS NOT NULL
)