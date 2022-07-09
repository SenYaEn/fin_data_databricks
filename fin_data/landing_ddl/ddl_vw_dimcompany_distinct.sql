-- Databricks notebook source
USE landing;

CREATE OR REPLACE VIEW vw_DimCompany_distinct AS

WITH ranked AS
(
    SELECT name,
    symbol,
    has_intraday,
    has_eod,
    country,
    exchangeMic,
    exchangeAcronym,
    ROW_NUMBER () OVER (PARTITION BY symbol ORDER BY dayId, runId DESC) AS rank_id
    FROM DimCompany
)

SELECT name,
       symbol,
       has_intraday,
       has_eod,
       country,
       exchangeMic,
       exchangeAcronym
FROM ranked
WHERE rank_id = 1