-- Databricks notebook source
USE landing;

CREATE OR REPLACE VIEW vw_DimExchange_distinct AS

WITH ranked AS
(
    SELECT name,
           acronym,
           mic,
           country,
           country_code,
           city,
           website,   
           currencyCode,
           timezoneName,
           timezoneAbbr,
           timezoneAbbrDst,
    ROW_NUMBER () OVER (PARTITION BY mic ORDER BY dayId, runId DESC) AS rank_id
    FROM DimExchange
)

SELECT name,
       acronym,
       mic,
       country,
       country_code,
       city,
       website,   
       currencyCode,
       timezoneName,
       timezoneAbbr,
       timezoneAbbrDst
FROM ranked
WHERE rank_id = 1