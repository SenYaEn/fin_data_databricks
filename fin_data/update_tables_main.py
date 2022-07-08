# Databricks notebook source
# MAGIC %run fin_data/functions_master

# COMMAND ----------

file_path = 'Config/config_dim_tables.yml'
notebook_base_path = '/'

# COMMAND ----------

update_tables (file_path, notebook_base_path)