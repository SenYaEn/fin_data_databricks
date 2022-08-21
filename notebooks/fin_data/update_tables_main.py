# Databricks notebook source
# MAGIC %run fin_data/functions_master

# COMMAND ----------

file_path = dbutils.widgets.get('file_path')
notebook_base_path = dbutils.widgets.get('notebook_base_path')

# COMMAND ----------

update_tables (file_path, notebook_base_path)