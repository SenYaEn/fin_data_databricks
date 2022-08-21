-- Databricks notebook source
USE landing;

CREATE OR REPLACE VIEW vw_FactDividends_distinct AS

WITH ranked AS
(
    SELECT `date`,
            dividend,
            symbol,
            pk,
    ROW_NUMBER () OVER (PARTITION BY pk ORDER BY dayId, runId DESC) AS rank_id
    FROM FactDividends
)

SELECT `date`,
        dividend,
        symbol,
        pk
FROM ranked
WHERE rank_id = 1