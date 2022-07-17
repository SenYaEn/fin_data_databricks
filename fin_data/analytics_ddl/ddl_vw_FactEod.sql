-- Databricks notebook source
DROP VIEW IF EXISTS analytics.vw_FactEod;

CREATE VIEW analytics.vw_FactEod AS
(
  SELECT  CONCAT(Symbol, '_', ExchangeId) AS CompanyExchangeId
        , Symbol AS CompanyId
        , Open
        , High
        , Low
        , Close
        , Volume
        , SplitFactor
        , Dividend
        , ExchangeId
        , `Date`
        , Close / Open - 1 AS OpenToCloseChange
        , High / Low -1 AS LowToHightChange
  FROM staging.FactEod
)