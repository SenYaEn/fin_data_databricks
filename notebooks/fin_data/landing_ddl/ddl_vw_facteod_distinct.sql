-- Databricks notebook source
USE landing;

CREATE OR REPLACE VIEW vw_FactEod_distinct AS

WITH ranked AS
(
    SELECT open,
           high,
           low,
           close,
           volume,
           adj_high,
           adj_low,
           adj_close,
           adj_open,
           adj_volume,
           split_factor,
           dividend,
           symbol,
           `exchange`,
           `date`,
           pk,
    ROW_NUMBER () OVER (PARTITION BY pk ORDER BY dayId, runId DESC) AS rank_id
    FROM FactEod
)

SELECT open,
       high,
       low,
       close,
       volume,
       adj_high,
       adj_low,
       adj_close,
       adj_open,
       adj_volume,
       split_factor,
       dividend,
       symbol,
       `exchange`,
       `date`,
       pk
FROM ranked
WHERE rank_id = 1