-- Databricks notebook source
DROP SCHEMA IF EXISTS staging;

CREATE SCHEMA staging 
COMMENT 'This is staging schema' LOCATION '/mnt/lake/Staging'