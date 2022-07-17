-- Databricks notebook source
DROP VIEW IF EXISTS analytics.vw_FactDividends;

CREATE VIEW analytics.vw_FactDividends AS
(
  SELECT  Symbol
        , Dividend
        , `Date`
  FROM staging.FactDividends
)