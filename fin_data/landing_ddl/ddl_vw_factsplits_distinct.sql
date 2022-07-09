-- Databricks notebook source
USE landing;

CREATE OR REPLACE VIEW vw_FactSplits_distinct AS

WITH ranked AS
(
    SELECT `date`,
            split_factor,
            symbol,
            pk,
    ROW_NUMBER () OVER (PARTITION BY pk ORDER BY dayId, runId DESC) AS rank_id
    FROM FactSplits
)

SELECT `date`,
        split_factor,
        symbol,
        pk
FROM ranked
WHERE rank_id = 1