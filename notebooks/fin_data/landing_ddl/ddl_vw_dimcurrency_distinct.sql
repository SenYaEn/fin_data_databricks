-- Databricks notebook source
USE landing;

CREATE OR REPLACE VIEW vw_DimCurrency_distinct AS

WITH ranked AS
(
    SELECT `code`,
            symbol,
            name,
    ROW_NUMBER () OVER (PARTITION BY `code` ORDER BY dayId, runId DESC) AS rank_id
    FROM DimCurrency
)

SELECT `code`,
        symbol,
        name
FROM ranked
WHERE rank_id = 1