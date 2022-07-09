# Databricks notebook source
import requests
import json
from pyspark.sql.functions import *
from datetime import date, datetime
from pyspark.sql.types import *
import pandas as pd
from delta.tables import *

import os
import tempfile
from envyaml import EnvYAML
from azure.storage.blob.aio import BlobClient
import asyncio

# COMMAND ----------

def get_total_api_count (access_key, base_url, path_params):
    """
    get_total_api_count gets the total number of records in the whole API response (not in a page, the whole response).
    
    :access_key: API Key
    :base_url: Base URL 
    :path_params: API request path
    
    :return: The function returns an integer which is the number of records in the whole API response.
    """
    params = {
      'access_key': access_key
    }
    api_result = requests.get(f"{base_url}/{path_params}", params) 
    api_response = api_result.json()
    try:
        total = api_response['pagination']['total']
        return total
    except:
        raise Exception(f"Function name: 'get_total_api_count', Error message: {api_response}")

# COMMAND ----------

def get_limit_and_offset (total, max_offset = 1000):
    """
    get_limit_and_offset determines the necessary limit and offset for an API request to ensure all requested data is loaded.
    
    :total: Total number of records in the whole API response (not in a page, the whole response)
    :max_offset: At present Marketstack has a limit of 1000 records per page so it's a default value for this parameter 
    
    :return: The function returns a dictionary with the limit and offset values.
    """
    params = {}
    
    if (total/max_offset) <= 1:
        params['limit'] = total
        params['offset'] = 0
    else:
        params['limit'] = max_offset
        params['offset'] = max_offset
    return params

# COMMAND ----------

def get_basic_api_json (base_url, path_params, access_key, limit, offset):
    """
    get_basic_api_json obtains all the records from the API response on the current page.
    
    :base_url: Base URL 
    :path_params: API request path
    :access_key: API Key
    :limit: This parameter is determined by the function which calculates the optimal limit per API request 
    :offset: Together with 'limit' parameter this parameter allows to flip through all the pages of the API response
    
    :return: The function returns a list of JSON objects.
    """
    params = {
        'access_key': access_key,
        'limit': limit,
        'offset': offset
    }
    
    api_result = requests.get(f"{base_url}/{path_params}", params) 
    api_response = api_result.json()
    
    try:
        api_json = api_response['data']
        return api_json
    except:
        raise Exception(f"Function name: 'get_basic_api_json', Error message: {api_response}")

# COMMAND ----------

def get_exchanges_df (api_json):
    """
    get_exchanges_df converts a diction with JSON objects into a dataframe. 
    
    :api_json: A list of JSON objects with all the records from an API response on a page 
    
    :return: The function returns a dataframe with flattened JSON input according to the pre-determined schema.
    """
    schema = StructType([
        StructField('name', StringType(), False),
        StructField('acronym', StringType(), True),
        StructField('mic', StringType(), False),
        StructField('country', StringType(), True),
        StructField('country_code', StringType(), True),
        StructField('city', StringType(), True),
        StructField('website', StringType(), True),
        StructField('timezone', StructType(
        [
            StructField('timezone', StringType(), True),
            StructField('abbr', StringType(), True),
            StructField('abbr_dst', StringType(), True)
        ]), True),
        StructField('currency', StructType(
        [
            StructField('code', StringType(), True),
            StructField('symbol', StringType(), True),
            StructField('name', StringType(), True)
        ]), True)
    ])
    
    try:
        df = (spark.createDataFrame(api_json, schema)
              .withColumn('currencyCode', col('currency.code'))
              .withColumn('timezoneName', col('timezone.timezone'))
              .withColumn('timezoneAbbr', col('timezone.abbr'))
              .withColumn('timezoneAbbrDst', col('timezone.abbr_dst'))
              .drop('currency', 'timezone', '_corrupt_record')
              .na.drop(subset=['mic']))
        return df
    except Exception as error:
        raise Exception(f"Function name: 'get_exchanges_df', Error message: {error}")

# COMMAND ----------

def get_eod_df(api_json):
    """
    get_eod_df converts a diction with JSON objects into a dataframe. 
    
    :api_json: A list of JSON objects with all the records from an API response on a page 
    
    :return: The function returns a dataframe with flattened JSON input according to the pre-determined schema.
    """
    schema = StructType([
        StructField('open', FloatType(), True),
        StructField('high', FloatType(), True),
        StructField('low', FloatType(), True),
        StructField('close', FloatType(), True),
        StructField('volume', FloatType(), True),
        StructField('adj_high', StringType(), True),
        StructField('adj_low', StringType(), True),
        StructField('adj_close', FloatType(), True),
        StructField('adj_open', StringType(), True),
        StructField('adj_volume', StringType(), True),
        StructField('split_factor', FloatType(), True),
        StructField('dividend', FloatType(), True),
        StructField('symbol', StringType(), False),
        StructField('exchange', StringType(), False),
        StructField('date', StringType(), False)
    ])
    
    try:
        df = spark.createDataFrame(api_json, schema)
        df = df.withColumn('date', date_format(to_timestamp('date'), "yyyyMMdd"))
        df = df.withColumn('pk', concat(df.symbol, lit('_'), df.date))
        df = df.withColumn('date', df.date.cast('int'))
        return df
    except Exception as error:
        raise Exception(f"Function name: 'get_eod_df', Error message: {error}")

# COMMAND ----------

def get_intraday_df (api_json):
    """
    get_intraday_df converts a diction with JSON objects into a dataframe. 
    
    :api_json: A list of JSON objects with all the records from an API response on a page 
    
    :return: The function returns a dataframe with flattened JSON input according to the pre-determined schema.
    """
    schema = StructType([
        StructField('open', FloatType(), False),
        StructField('high', FloatType(), False),
        StructField('low', FloatType(), False),
        StructField('last', FloatType(), True),
        StructField('close', FloatType(), True),
        StructField('volume', FloatType(), True),
        StructField('date', StringType(), False),
        StructField('symbol', StringType(), False),
        StructField('exchange', StringType(), False)
    ])
    
    try:
        df = spark.createDataFrame(api_json, schema)
        return df
    except Exception as error:
        raise Exception(f"Function name: 'get_intraday_df', Error message: {error}")

# COMMAND ----------

def get_dividends_df (api_json):
    """
    get_dividends_df converts a diction with JSON objects into a dataframe. 
    
    :api_json: A list of JSON objects with all the records from an API response on a page 
    
    :return: The function returns a dataframe with flattened JSON input according to the pre-determined schema.
    """
    schema = StructType([
        StructField('date', StringType(), False),
        StructField('dividend', FloatType(), False),
        StructField('symbol', StringType(), False)
    ])
    
    try:
        df = spark.createDataFrame(api_json, schema)
        df = df.withColumn('date', date_format(to_timestamp('date'), "yyyyMMdd"))
        df = df.withColumn('pk', concat(df.symbol, lit('_'), df.date))
        return df
    except Exception as error:
        raise Exception(f"Function name: 'get_dividends_df', Error message: {error}")

# COMMAND ----------

def get_splits_df (api_json):
    """
    get_splits_df converts a diction with JSON objects into a dataframe. 
    
    :api_json: A list of JSON objects with all the records from an API response on a page 
    
    :return: The function returns a dataframe with flattened JSON input according to the pre-determined schema.
    """
    schema = StructType([
        StructField('date', StringType(), False),
        StructField('split_factor', FloatType(), False),
        StructField('symbol', StringType(), False)
    ])
    
    try:
        df = spark.createDataFrame(api_json, schema)
        df = df.withColumn('date', date_format(to_timestamp('date'), "yyyyMMdd"))
        df = df.withColumn('pk', concat(df.symbol, lit('_'), df.date))
        df = df.withColumn('date', df.date.cast('int'))
        return df
    except Exception as error:
        raise Exception(f"Function name: 'get_splits_df', Error message: {error}")

# COMMAND ----------

def get_company_df (api_json):
    """
    get_company_df converts a diction with JSON objects into a dataframe. 
    
    :api_json: A list of JSON objects with all the records from an API response on a page 
    
    :return: The function returns a dataframe with flattened JSON input according to the pre-determined schema.
    """
    schema = StructType([
        StructField('name', StringType(), True),
        StructField('symbol', StringType(), False),
        StructField('has_intraday', BooleanType(), True),
        StructField('has_eod', BooleanType(), True),
        StructField('country', StringType(), True),
        StructField('stock_exchange', StructType(
        [
            StructField('name', StringType(), True), 
            StructField('acronym', StringType(), True),
            StructField('mic', StringType(), True),
            StructField('country', StringType(), True),
            StructField('country_code', StringType(), True),
            StructField('city', StringType(), True),
            StructField('website', StringType(), True)
        ]), True)
    ])
    
    try:
        df = spark.createDataFrame(api_json, schema)
        df = (df.withColumn('exchangeMic', col('stock_exchange.mic'))
              .withColumn('exchangeAcronym', col('stock_exchange.acronym'))
              .drop('stock_exchange'))
        return df
    except Exception as error:
        raise Exception(f"Function name: 'get_company_df', Error message: {error}")

# COMMAND ----------

def get_currencies_df (api_json):
    """
    get_currencies_df converts a diction with JSON objects into a dataframe. 
    
    :api_json: A list of JSON objects with all the records from an API response on a page 
    
    :return: The function returns a dataframe with flattened JSON input according to the pre-determined schema.
    """
    schema = StructType([
        StructField('code', StringType(), False),
        StructField('symbol', StringType(), False),
        StructField('name', StringType(), False)
    ])
    
    try:
        df = spark.createDataFrame(api_json, schema)
        return df
    except Exception as error:
        raise Exception(f"Function name: 'get_currencies_df', Error message: {error}")

# COMMAND ----------

def get_timezones_df (api_json):
    """
    get_timezones_df converts a diction with JSON objects into a dataframe. 
    
    :api_json: A list of JSON objects with all the records from an API response on a page 
    
    :return: The function returns a dataframe with flattened JSON input according to the pre-determined schema.
    """
    schema = StructType([
        StructField('timezone', StringType(), False),
        StructField('abbr', StringType(), False),
        StructField('abbr_dst', StringType(), False)
    ])
    
    try:
        df = spark.createDataFrame(api_json, schema)
        return df
    except Exception as error:
        raise Exception(f"Function name: 'get_timezones_df', Error message: {error}")

# COMMAND ----------

def write_api_to_adl (base_url, path_params, access_key, limit, offset, destination_folder, run_date, run_id):
    """
    write_api_to_adl writes a specific dataframe to a Data Lake folder. 
    
    :base_url: Base URL 
    :path_params: API request path
    :access_key: API Key
    :limit: This parameter is determined by the function which calculates the optimal limit per API request 
    :offset: Together with 'limit' parameter this parameter allows to flip through all the pages of the API response
    :destination_folder: Data Lake folder path (e.g. 'Landing/Eod')
    :run_date: Date of the run in integer format (e.g. 20220706)
    :run_id: Run number per run_date.
    """
    api_json = get_basic_api_json (base_url, path_params, access_key, limit, offset)
    if 'Exchanges' in destination_folder:
        landing_df = get_exchanges_df (api_json)
    elif 'Eod' in destination_folder:
        landing_df = get_eod_df (api_json)
    elif 'Companies' in destination_folder:
        landing_df = get_company_df (api_json)
    elif 'Currencies' in destination_folder:
        landing_df = get_currencies_df (api_json)
    elif 'Timezones' in destination_folder:
        landing_df = get_timezones_df (api_json)
    elif 'Intraday' in destination_folder:
        landing_df = get_intraday_df (api_json)
    elif 'Dividends' in destination_folder:
        landing_df = get_dividends_df (api_json)
    elif 'Splits' in destination_folder:
        landing_df = get_splits_df (api_json)  

    landing_df.write.mode('append').parquet(f"/mnt/lake/{destination_folder}/dayId={run_date}/runId={run_id}")

# COMMAND ----------

def get_table_location (table_name):
    """
    get_table_location retrieves Hive table location and its directory (which is a subset of a full location). 

    :table_name: Full table name including database name (e.g. landing.DimCompany)
    
    :return: The function returns a dictionary with table's full location and directory.
    """    
    location = {}
    
    try:
        location['full_location'] = spark.sql(f"DESCRIBE FORMATTED {table_name}").where(col('col_name') == 'Location').select('data_type').collect()[0][0]
        location['directory'] = location['full_location'].replace('dbfs:/mnt/lake/', '')
        return location
    except Exception as error:
        raise Exception(f"Function name: 'get_table_location', Error message: {error}")

# COMMAND ----------

def get_max_day_and_run_id (table_name):
    """
    get_max_day_and_run_id retrieves max dayId partition and its corresponding max runId. 

    :table_name: Full table name including database name (e.g. landing.DimCompany)
    
    :return: The function returns a dictionary with dayId and runId values.
    """      
    try:
        df = spark.sql(f"SHOW PARTITIONS {table_name}")
        
        partitions = {}
        
        if df.count() > 0:
            df = (df.withColumn('split_1', split(df['partition'], '/')[0])
                  .withColumn('split_2', split(df['partition'], '/')[1]))
            df = (df.withColumn('dayId', split(df['split_1'], '=')[1])
                  .withColumn('runId', split(df['split_2'], '=')[1]))
            
            df = df.select('dayId','runId').sort(['dayId','runId'], ascending=False)
            
            partitions['max_day_id'] = df.collect()[0][0]
            partitions['max_run_id'] = df.collect()[0][1]
            return partitions
        else:
            raise Exception(f"Table {table_name} has no partition values to return.")
    
    except Exception as error:
        raise Exception(f"Function name: 'get_max_day_and_run_id', Error message: {error}")

# COMMAND ----------

def get_next_run_id (table_name, date):
    """
    get_next_run_id determines the next runId for a given day. 

    :table_name: Full table name including database name (e.g. landing.DimCompany)
    :date: Date as an integer (e.g. 20220101)
    
    :return: The function returns an integer nex runId.
    """     
    try:
        df = spark.sql(f"SHOW PARTITIONS {table_name}")
        df = (df.withColumn('split_1', split(df['partition'], '/')[0])
              .withColumn('split_2', split(df['partition'], '/')[1]))
        df = (df.withColumn('dayId', split(df['split_1'], '=')[1])
              .withColumn('runId', split(df['split_2'], '=')[1]))
        
        date_exists = bool(df.filter(df.dayId.contains(date)).collect())
        
        if date_exists == True:
            next_run_id = df.where(col('dayId') == int(date)).select('dayId','runId').sort(['runId'], ascending=False).collect()[0][1]
            next_run_id = int(next_run_id) + 1
            next_run_id = str(next_run_id)
        else:
            next_run_id = '1'
            
        return next_run_id
    
    except Exception as error:
        raise Exception(f"Function name: 'get_next_run_id', Error message: {error}")

# COMMAND ----------

def write_api_to_adl_umbrella (access_key, base_url, path_params, table_name, run_date, run_id):
    """
    write_api_to_adl_umbrella determines how many API calls need to be performed to get all the response pages. 

    :access_key: API Key
    :base_url: Base URL 
    :path_params: API request path
    :table_name: Full table name including database name (e.g. landing.DimCompany)
    :run_date: Date of the run in integer format (e.g. 20220706)
    :run_id: Run number per run_date.
    """
    destination_folder = get_table_location (table_name)['directory']
    total = get_total_api_count (access_key, base_url, path_params)
    limit = get_limit_and_offset (total)['limit']
    offset = 0
    
    # If limit equals to total (as determined by get_limit_and_offset) then only a single API call needs to be made
    if limit == total:
        
        write_api_to_adl (base_url, path_params, access_key, limit, offset, destination_folder, run_date, run_id)
    
    # If limit is smaller then total it means number of records per page exceed the max offset and several API calls need to be made to get through all response pages        
    elif limit < total:
        while total > 0:
            
            write_api_to_adl (base_url, path_params, access_key, limit, offset, destination_folder, run_date, run_id)
            
            # After each call the 'total' variable will decrease and offset will shift to gradually get all the records in the response
            total = total-limit
            offset = offset + limit
    
    repair_table (table_name)
    
    return print(f"Data written to {destination_folder} folder, dayId {run_date} | runId {run_id}")

# COMMAND ----------

def merge_into_delta_table (destination_table, source_table, merge_keys, column_mapping, is_full_reload):
    """
    merge_into_delta_table merges records from source table into a Delta table. 

    :destination_table: Full table name including database (e.g. staging.FactDevidend)
    :source_table: Full table name including database (e.g. landing.FactDevidend) 
    :merge_keys: Unique keys in both tables based on which the whole row can be updated or inserted
    :column_mapping: Dictionary with mapping between fields from source and destination tables
    :is_full_reload: Boolean flag indicating if all unique records from source table need to be merged or only the latest partition.
    """   
    if is_full_reload==True:
        # Here source is a view where duplicates are eliminated by taking a row with the latest partitions
        source_df = spark.sql(f"SELECT * FROM {source_table.split('.')[0]}.vw_{source_table.split('.')[1]}_distinct")
    else:
        # First get the most recent date and run id for that date
        max_partition = get_max_day_and_run_id (source_table)
        source_filter = f"WHERE dayId = {max_partition['max_day_id']} AND runId = {max_partition['max_run_id']}"
        source_df = spark.sql(f"SELECT * FROM {source_table} {source_filter}")
    
    destination_directory = get_table_location (destination_table)['full_location'].replace('dbfs:', '')  
    destination_delta_table = DeltaTable.forPath(spark, destination_directory)
    
    # Merge data into a Delta table
    try:
        (destination_delta_table.alias('destination')
         .merge(source_df.alias('updates'), f"{merge_keys}")
         .whenMatchedUpdate(set = column_mapping)
         .whenNotMatchedInsert(values = column_mapping)
         .execute())
        print(f"{destination_table} has been updated")
    except Exception as error:
        raise Exception(f"Function name: 'merge_into_delta_table', Error message: {error}")

# COMMAND ----------

def get_companies_per_exchange (exchanges):
    """
    get_companies_per_exchange gets a list of all the companies associated with a given stock exchange(s). 

    :exchanges: A string value with one or more stock exchange keys

    :return: List of string items with company keys.
    """       
    try:
        if "'" in exchanges:
            # If input value is multiple exchange keys then quotation marks in the IN clause are NOT needed
            companies = (list(spark.sql(f"SELECT CompanyId FROM staging.DimCompany WHERE ExchangeId IN ({exchanges})").toPandas()['CompanyId']))
        else:
            # If input value is a single exchange key then quotation marks in the IN clause ARE needed
            companies = (list(spark.sql(f"SELECT CompanyId FROM staging.DimCompany WHERE ExchangeId IN ('{exchanges}')").toPandas()['CompanyId']))
        return companies
    except Exception as error:
        raise Exception(f"Function name: 'get_companies_per_exchange', Error message: {error}")

# COMMAND ----------

def list_to_string (list_name):
    """
    list_to_string converts list into a string. 

    :list_name: List of items

    :return: String without square brackets.
    """   
    string_output = str(list_name).replace("[", "").replace("]", "")
    return string_output

# COMMAND ----------

def int_to_date (date_int):
    """
    int_to_date converts date in integer format into date. 

    :list_name: Date in integer format (e.g. 20220706)

    :return: Date in yyyy-mm-dd format (e.g. 2022-07-06)
    """  
    date = str(date_int)
    date = datetime(year=int(date[0:4]), month=int(date[4:6]), day=int(date[6:8])).strftime("%Y-%m-%d")
    return date

# COMMAND ----------

def get_min_max_date (exchanges, table_name, history_start_date):
    """
    get_min_max_date first determines a max date for each company and then takes the min date from that set. 

    :exchanges: A string value with one or more stock exchange keys
    :table_name: Full table name (e.g. 'staging.FactEod')
    :history_start_date: A default start date to start pulling records from

    :return: Date in integer format (e.g. 20210101)
    """  
    
    # First get companies essociated with a given exchange(s) from DimCompany table (which is supposed to have the most complete list of companies and their stock exchanges)
    dim_companies = get_companies_per_exchange (exchanges)
    companies = list_to_string (dim_companies)
    
    try:
        # Secondly get distinct list of same (it could be the Fact table doesn't yet have all the companies DimCompany has) companies in a Fact table
        fact_companies = list(spark.sql(f"SELECT DISTINCT Symbol FROM {table_name} WHERE Symbol IN ({companies})").toPandas()['Symbol'])
    except Exception as error:
        raise Exception(f"Function name: 'get_min_max_date', Error message: {error}")
    
    # Thirdly subtract list of companies from Fact table from that from DimCompany to check if Fact table is missing some companies
    difference = list(set(dim_companies) - set(fact_companies))
    
    # If Fact table IS missing some companies then the whole set of companies will get all the records starting from the default history_start_date date
    if len(difference) > 0:
        min_max_date = history_start_date
    else:
        try:
            # If Fact table is NOT missing any companies then max date for each company will be found
            companies_dates = spark.sql(f"""SELECT Symbol, MAX(`Date`) AS `Date` FROM {table_name} WHERE Symbol IN ({companies}) GROUP BY Symbol""")
        except Exception as error:
            raise Exception(f"Function name: 'get_min_max_date', Error message: {error}")
        
        # Find a single min date amongst all the max dates per company
        min_max_date = str(companies_dates.agg({'Date': 'min'}).collect()[0][0])
        min_max_date = int_to_date (min_max_date)
        
    return min_max_date

# COMMAND ----------

def repair_table (table_name):
    """
    repair_table runs MSCK REPAIR TABLE on a partitioned table. 

    :table_name: Full table name (e.g. 'staging.FactEod')

    :return: A string success message.
    """ 
    try:
        spark.sql(f"MSCK REPAIR TABLE {table_name}")
        return print(f"{table_name} repaired successfully")
    except Exception as error:
        raise Exception(f"Function name: 'repair_table', Error message: {error}")

# COMMAND ----------

def run_ddl_notebook (database_name, table_name, notebook_base_path):
    """
    run_ddl_notebook runs a notebook at a specified path. 
    
    :database_name: Hive database name (e.g. 'landing')
    :table_name: Table name without database (e.g. 'FactEod')
    :notebook_base_path: A string path in case if a notebook is located in an individual user's workspace (e.g. /Users/user@email.com')
    """     
    path = f"{notebook_base_path}/fin_data/{database_name.lower()}_ddl"
    
    parameters = {}
    parameters['landing_database_name'] = database_name.lower()
    
    dbutils.notebook.run(f"{path}/ddl_{database_name.lower()}_{table_name.lower()}", 60, parameters)

# COMMAND ----------

def write_api_to_landing_fact_tables (access_key, base_url, exchanges, table_name, endpoint_name, history_start_date):
    """
    write_api_to_landing_fact_tables makes API calls and writes data to Data Lake for Fact tables that have the same API path structure.

    :access_key: API Key
    :base_url: Base URL
    :exchanges: A string value with one or more stock exchange keys
    :table_name: Full table name including database name (e.g. landing.FactEod)
    :endpoint_name: Endpoint defining API call for a given Fact table (e.g. 'eod')
    :history_start_date: Date in yyyy-MM-dd format.
    """    
    run_date = datetime.now().strftime("%Y%m%d")
    run_id = get_next_run_id (table_name, run_date)  
    
    staging_table = table_name.replace('landing', 'staging')
    
    companies = get_companies_per_exchange (exchanges)[:200]
#     companies = get_companies_per_exchange (exchanges)
    companies_count = len(companies)
    
    # Marketstack API has a limit of how many symbols (i.e. company codes) can be included in a single API request. 99 is chosen instead of 100 just to be on the safe side.
    # If the list of companies has more than 99 items it will be split into chuncks of 99 items (the last chunk in most cases would be less than 99) and separate API requests will be sent.
    if companies_count > 99:
    
        array_start = 0
        array_end = 99
        
        while companies_count > 0:
        
            companies_api = companies[array_start:array_end]
            companies_api = list_to_string (companies_api).replace("'", "")
            start_date = get_min_max_date (exchanges=exchanges, history_start_date=history_start_date, table_name=staging_table)
            end_date = datetime.now().strftime("%Y-%m-%d")
            path_params = f"{endpoint_name}?symbols={companies_api}&date_from={start_date}&date_to={end_date}"
            
            write_api_to_adl_umbrella (access_key, base_url, path_params, table_name, run_date, run_id)
        
            array_start = array_start + 99
            array_end = array_end + 99
        
            companies_count = companies_count -99
            
    # If the list of companies has less or 99 items a single API requests will be sent.        
    else:
        companies_api = list_to_string (companies).replace("'", "")
        start_date = get_min_max_date (exchanges=exchanges, history_start_date=history_start_date, table_name=staging_table)
        end_date = datetime.now().strftime("%Y-%m-%d")
        path_params = f"{endpoint_name}?symbols={companies_api}&date_from={start_date}&date_to={end_date}"
        
        write_api_to_adl_umbrella (access_key, base_url, path_params, table_name, run_date, run_id)

# COMMAND ----------

def write_api_to_landing_dim_tables (access_key, base_url, table_name, exchange=''):
    """
    write_api_to_landing_dim_tables makes API calls and writes data to Data Lake for Dim tables that have the same or similar API path structure.

    :access_key: API Key
    :base_url: Base URL
    :table_name: Full table name including database name (e.g. landing.DimCompany)
    :exchange: A string value with one stock exchange key. Only applicable to DimCompany.
    """  
    run_date = datetime.now().strftime("%Y%m%d")
    run_id = get_next_run_id (table_name, run_date)
    
    if 'DimCompany' in table_name:
        path_params = f"tickers?exchange={exchange}"
    elif 'DimExchange' in table_name:
        path_params = 'exchanges'
    elif 'DimCurrency' in table_name:
        path_params = 'currencies'
    
    write_api_to_adl_umbrella (access_key, base_url, path_params, table_name, run_date, run_id)

# COMMAND ----------

def write_aip_to_staging_dated_fact_tables (access_key, base_url, landing_database_name, staging_database_name, table_name, exchanges, merge_keys, column_mapping, notebook_base_path, endpoint_name, history_start_date, is_full_reload):
    """
    write_aip_to_staging_dated_fact_tables makes API calls, writes data to Data Lake, and merges data into Delta staging table for Fact tables that have the same API path structure.

    :access_key: API Key
    :base_url: Base URL
    :landing_database_name: Hive database name
    :staging_database_name: Hive database name
    :table_name: Table name without database name (e.g. FactEod)
    :exchanges: A string value with one or more stock exchange keys
    :merge_keys: Unique keys in both tables based on which the whole row can be updated or inserted
    :column_mapping: Dictionary with mapping between fields from source and destination tables
    :notebook_base_path: A string path in case if a notebook is located in an individual user's workspace (e.g. /Users/user@email.com')
    :endpoint_name: Endpoint defining API call for a given Fact table (e.g. 'eod')
    :history_start_date: Date in yyyy-MM-dd format
    :is_full_reload: Boolean flag indicating if all unique records from source table need to be merged or only the latest partition
    """ 
    source_table = f"{landing_database_name}.{table_name}"
    destination_table = f"{staging_database_name}.{table_name}"
        
    write_api_to_landing_fact_tables (access_key=access_key, 
                                      base_url=base_url, 
                                      exchanges=exchanges, 
                                      table_name=source_table, 
                                      endpoint_name=endpoint_name, 
                                      history_start_date=history_start_date)
    
    run_ddl_notebook (database_name=landing_database_name, 
                      table_name=table_name, 
                      notebook_base_path=notebook_base_path)
    
    merge_into_delta_table (destination_table=destination_table, 
                            source_table=source_table, 
                            merge_keys=merge_keys, 
                            column_mapping=column_mapping,
                            is_full_reload=is_full_reload)

# COMMAND ----------

def write_aip_to_staging_dim_tables (access_key, base_url, landing_database_name, staging_database_name, table_name, merge_keys, column_mapping, is_full_reload, exchange='', notebook_base_path='/'):
    """
    write_aip_to_staging_dim_tables makes API calls, writes data to Data Lake, and merges data into Delta staging table for Dim tables that have the same or similar API path structure.

    :access_key: API Key
    :base_url: Base URL
    :landing_database_name: Hive database name
    :staging_database_name: Hive database name
    :table_name: Table name without database name (e.g. DimExchange)
    :merge_keys: Unique keys in both tables based on which the whole row can be updated or inserted
    :column_mapping: Dictionary with mapping between fields from source and destination tables
    :is_full_reload: Boolean flag indicating if all unique records from source table need to be merged or only the latest partition
    :exchange: A string value one stock exchange key
    :notebook_base_path: A string path in case if a notebook is located in an individual user's workspace (e.g. /Users/user@email.com')
    """ 
    source_table = f"{landing_database_name}.{table_name}"
    destination_table = f"{staging_database_name}.{table_name}"
    
    write_api_to_landing_dim_tables (access_key=access_key, 
                                     base_url=base_url, 
                                     table_name=source_table, 
                                     exchange=exchange)
    
    run_ddl_notebook (database_name=landing_database_name, 
                      table_name=table_name, 
                      notebook_base_path=notebook_base_path)
    
    merge_into_delta_table (destination_table=destination_table, 
                            source_table=source_table, 
                            merge_keys=merge_keys, 
                            column_mapping=column_mapping, 
                            is_full_reload=is_full_reload)

# COMMAND ----------

async def download_config_file (file_path):
    """
    download_config_file downloads YAML config file from Blob Storage.

    :file_path: File path in Data Lake (e.g. 'Config/config_dim_tables.yml')
    """     
    try:
        account = dbutils.secrets.get(scope = 'DevOpsSecret', key = 'AdlAccountName')
        key = dbutils.secrets.get(scope = 'DevOpsSecret', key = 'AdlAccountSecret')
        container = dbutils.secrets.get(scope = 'DevOpsSecret', key = 'AdlMainContainerName')
        
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={account};AccountKey={key};EndpointSuffix=core.windows.net"
        blob_client = BlobClient.from_connection_string(conn_str=connection_string, 
                                                        container_name=container, 
                                                        blob_name=file_path)
        config_file = os.path.basename(file_path)
        file_path = os.path.join(tempfile.mkdtemp(), config_file)
        
        with open(file_path, "wb") as download_file:
            stream = await blob_client.download_blob()
            data = await stream.readall()
            download_file.write(data)
            await blob_client.close()
        return file_path
  
    except Exception as error:
        raise Exception(f"Function name: 'download_config_file', Error message: {error}")

# COMMAND ----------

def get_yaml_file (file_path):
    """
    get_yaml_file calls the function that downloads a YAML file from Blob Storage.

    :file_path: File path in Data Lake (e.g. 'Config/config_dim_tables.yml')
    """
    downloaded_file = asyncio.run(download_config_file (file_path))
    try:
        config_object = EnvYAML(downloaded_file)
        return config_object
    except Exception as error:
        raise Exception(f"Function name: 'get_yaml_file', Error message: {error}")

# COMMAND ----------

def update_dim_tables (tables, access_key, base_url, landing_database_name, staging_database_name, notebook_base_path):
    """
    update_dim_tables retrieves parameters for Dim tables from YAML items and runs a data extract from API call to merge into Delta staging table.
    
    :tables: List of YALM items with parameters for Dim tables
    :access_key: API Key
    :base_url: Base URL
    :landing_database_name: Hive database name
    :staging_database_name: Hive database name
    :notebook_base_path: A string path in case if a notebook is located in an individual user's workspace (e.g. /Users/user@email.com')
    """     
    try:
        for table in tables:
            if table['is_enabled']==True:
                table_name = table['table_name']
                exchange = table['exchange']
                merge_keys = table['merge_keys']
                column_mapping = table['column_mapping']
                is_full_reload = table['is_full_reload']
                write_aip_to_staging_dim_tables (access_key=access_key, 
                                                 base_url=base_url, 
                                                 landing_database_name=landing_database_name, 
                                                 staging_database_name=staging_database_name, 
                                                 table_name=table_name, 
                                                 merge_keys=merge_keys, 
                                                 column_mapping=column_mapping,
                                                 is_full_reload=is_full_reload,
                                                 exchange=exchange, 
                                                 notebook_base_path=notebook_base_path)
    except Exception as error:
        raise Exception(f"Function name: 'update_dim_tables', Error message: {error}")

# COMMAND ----------

def update_fact_tables (tables, access_key, base_url, landing_database_name, staging_database_name, notebook_base_path):
    """
    update_fact_tables retrieves parameters for Fact tables from YAML items and runs a data extract from API call to merge into Delta staging table.
    
    :tables: List of YALM items with parameters for Fact tables
    :access_key: API Key
    :base_url: Base URL
    :landing_database_name: Hive database name
    :staging_database_name: Hive database name
    :notebook_base_path: A string path in case if a notebook is located in an individual user's workspace (e.g. /Users/user@email.com')
    """        
    try:
        for table in tables:
            if table['is_enabled']==True:
                table_name = table['table_name']
                endpoint_name = table['endpoint_name']
                history_start_date = table['history_start_date']
                exchanges = table['exchanges']
                merge_keys = table['merge_keys']
                column_mapping = table['column_mapping']
                is_full_reload = table['is_full_reload']
                write_aip_to_staging_dated_fact_tables (access_key=access_key, 
                                                        base_url=base_url, 
                                                        landing_database_name=landing_database_name, 
                                                        staging_database_name=staging_database_name, 
                                                        table_name=table_name, 
                                                        exchanges=exchanges, 
                                                        merge_keys=merge_keys, 
                                                        column_mapping=column_mapping, 
                                                        notebook_base_path=notebook_base_path, 
                                                        endpoint_name=endpoint_name, 
                                                        history_start_date=history_start_date,
                                                        is_full_reload=is_full_reload)
    except Exception as error:
        raise Exception(f"Function name: 'update_fact_tables', Error message: {error}")

# COMMAND ----------

def update_tables (file_path, notebook_base_path):
    """
    update_tables retrieves general ETL parameters and then determines if Dim or Fact function should be executed.

    :file_path: File path in Data Lake (e.g. 'Config/config_dim_tables.yml')
    :notebook_base_path: A string path in case if a notebook is located in an individual user's workspace (e.g. /Users/user@email.com')
    """
    config_file = get_yaml_file (file_path)
    
    try:
        access_key = dbutils.secrets.get(scope = 'DevOpsSecret', key = 'MarketstackAccessKey')
        base_url = config_file['general']['base_url']
        landing_database_name = config_file['general']['landing_database_name']
        staging_database_name = config_file['general']['staging_database_name']
        tables = config_file['tables']
        
        if config_file['table_type']=='Dim':
            update_dim_tables (tables, access_key, base_url, landing_database_name, staging_database_name, notebook_base_path)
        elif config_file['table_type']=='Fact':
            update_fact_tables (tables, access_key, base_url, landing_database_name, staging_database_name, notebook_base_path)
    
    except Exception as error:
        raise Exception(f"Function name: 'update_tables', Error message: {error}")