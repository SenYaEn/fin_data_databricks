-- Databricks notebook source
DROP SCHEMA IF EXISTS landing;

CREATE SCHEMA landing 
COMMENT 'This is landing schema' LOCATION '/mnt/lake/Landing'