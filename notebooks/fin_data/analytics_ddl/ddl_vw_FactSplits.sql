-- Databricks notebook source
DROP VIEW IF EXISTS analytics.vw_FactSplits;

CREATE VIEW analytics.vw_FactSplits AS
(
  SELECT Symbol
       , SplitFactor 
       , `Date`
  FROM staging.FactSplits
)