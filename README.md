# fin_data_databricks: Market data ETL

This repository serves for extraction, transformation, and loading Marketstack API data into a set of fact and dimention tables on Azure Data Lake for further analysis by Power BI.

Marketstack API documentation can be found [here](https://marketstack.com/documentation)

## Infrastructure
The solution runs within the Azure platform and uses the following resources:
1. Databricks
2. Data Lake
3. Key Vault
4. Data Factory
5. Power BI
6. Hive Metastore
7. Delta Lake
8. Python
9. SQL

Below is a High-Level solution architechture diagram.

![image](https://user-images.githubusercontent.com/58121577/181736290-f7e04945-de73-4fdf-b85f-fd895c38fcf9.png)

### Databricks
A standard Cluster will suffice to run the solution and can be scaled up depending on the processing speed requirements. For a demo run the following cluster type was used:

![image](https://user-images.githubusercontent.com/58121577/181736531-e285017f-b806-4605-8381-a0531cb90464.png)

The following initiation script needs to run when the cluster is started:

```powershell
pip install aiohttp==3.8.1
pip install envyaml==1.6.201229
pip install azure-storage-blob==12.9.0
```
### Data Lake
Data Lake needs to be mounted on Databricks. This can be done using Python:

```python
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "xxxxxxxxxxxxxxxxxxxxxxxxxx",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="DevOpsSecret",key="DatabricksAdlSecret"),
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/5e6c8edd-8f40-4f40-bf71-72beeb50a43c/oauth2/token"}

dbutils.fs.mount(
  source = "abfss://lake@xxxxxxxxx.dfs.core.windows.net/",
  mount_point = "/mnt/lake",
  extra_configs = configs)
```

### Key Vault
Key Vault is used within this solution to store the API key, Data Lake credentials, and Databricks credentials. The Vault is mounted on Databricks using [Secret Scopes](https://docs.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes).

### Data Factory
Data Factory is used only for scheduling and triggering a single Databricks notebook which runs a full cycle ETL - from performing API calls to merging the data into Staging Delta tables. Data Factory code is linked to a separate [repo](https://github.com/SenYaEn/fin_data_adf).

### Power BI
Power BI is a potential consumer of the Fact and Dimention tables produced by this solution. Below are some sample Power BI dashboards that could be built using the data:

![image](https://user-images.githubusercontent.com/58121577/181608305-86363456-0212-429f-bd35-c1b3a7e44f3f.png)

![image](https://user-images.githubusercontent.com/58121577/181607457-cd8593a1-5aa5-47fb-bd7a-a3648b199ac7.png)

### Hive Metastore
Hive Metastore is used to store tables and views. Tables are use PARQUET to store files.

### Delta Lake
Delta Lake provides ACID transactions, scalable metadata handling, and unifies streaming and batch data processing. Within this solution Delta Lake tables are used in Staging layer (see the Conceptual design section) only.

### Python
The core functionality of this solution is written in Python (using PySpark interface).

### SQL
SQL is mainly used in table and view definitions. It is also occasionally used within Python functions like in ```get_companies_per_exchange```.

## Software Design
Architecture of the solution in this repo is best described using the Extract, Transform, and Load (ETL) framework. All Python functions involved in the process are located in the [functions_master.py](https://github.com/SenYaEn/fin_data_databricks/blob/main/fin_data/functions_master.py) file.

### Extract
The extraction process starts with retrieving batch config from a YAML file stored in Data Lake. Complete config files for this solution are stored in the ADF repo in the [configFiles folder](https://github.com/SenYaEn/fin_data_adf/tree/main/fin-data-adf/configFiles).

Below is a sample YAML config file:

```yaml
general:
    access_key_name: 'MarketstackAccessKey'
    base_url: 'https://api.marketstack.com/v1'
    landing_database_name: 'landing'
    staging_database_name: 'staging'
table_type: 'Fact'
tables:
    -
        is_enabled: True
        is_full_reload: False
        endpoint_name: 'dividends'
        history_start_date: '2010-01-01'
        table_name: 'FactDividends'
        exchanges: 'XTAI'
        merge_keys: 'destination.PK = updates.pk'
        column_mapping: {"PK": "updates.pk", "Symbol": "updates.symbol", "Dividend": "updates.dividend", "`Date`": "updates.`date`"}
```
The YAML config informs which API calls need to be performed.

Functions responsible for processing the config file are the following: 

```download_config_file``` : downloads YAML config file from Blob Storage

```get_yaml_file``` : calls the function that downloads a YAML file from Blob Storage

To perform API calls to Marketstack platform an API key is required. More information on how to obtain the key can be found on the [Marketstack website](https://marketstack.com).

Functions directly involved in creating and performing API calls are the following (more detailed documentation for each function is attached to each function definition within the code):

```get_total_api_count``` : gets the total number of records in the whole API response (not in a page, the whole response)

```get_limit_and_offset``` : determines the necessary limit and offset for an API request to ensure all requested data is loaded

```get_basic_api_json``` : obtains all the records from the API response on the current page

```write_api_to_landing_fact_tables``` : makes API calls and writes data to Data Lake for Fact tables that have the same API path structure

```write_api_to_landing_dim_tables``` : makes API calls and writes data to Data Lake for Dim tables that have the same or similar API path structure

```write_api_to_adl_umbrella``` : determines how many API calls need to be performed to get all the response pages

### Transform
The main part of data transformation within the solution is parsing API JSON payloads into flat structures.

Functions responsible for this step are:

```get_exchanges_df``` : converts a dictionary with JSON objects into a dataframe

```get_eod_df``` : converts a dictionary with JSON objects into a dataframe

```get_intraday_df``` : converts a dictionary with JSON objects into a dataframe

```get_dividends_df``` : converts a dictionary with JSON objects into a dataframe

```get_splits_df``` : converts a dictionary with JSON objects into a dataframe

```get_company_df``` : converts a dictionary with JSON objects into a dataframe

```get_currencies_df``` : converts a dictionary with JSON objects into a dataframe

```get_timezones_df``` : converts a dictionary with JSON objects into a dataframe

### Load
The flattened data is written to Azure Data Lake (ADL) in PARQUET format. 

There are three conceptual layers data goes through:

- Landing
- Staging
- Analytics

*Landing* and *Staging* are physical layers in ADL and each have their own folder. *Analytics* is only implemented as a set of (Hive) views on top of Staging (Hive) tables.

In *Landing* each table has it's folder and each folder is further split into dayId and runId folders. In the example below the Dividends API call was performed 4 times on a given date (20220703):

![image](https://user-images.githubusercontent.com/58121577/181736934-96f79e23-0ef4-4d44-a26e-20aaafbc3dd7.png)

Within the Landing layer data might be duplicated across dayId and runId folders. When data is merged into a Staged table only unique rows are selected (more details will be provided further).

Schemas for Landing tables are stored in Hive Metastore and defined using [SQL DDL](https://github.com/SenYaEn/fin_data_databricks/tree/main/fin_data/landing_ddl):

```sql
DROP TABLE IF EXISTS $landing_database_name.DimExchange;

CREATE TABLE $landing_database_name.DimExchange (
  name STRING,
  acronym STRING,
  mic STRING,
  country STRING,
  country_code STRING,
  city STRING,
  website STRING,   
  currencyCode STRING,
  timezoneName STRING,
  timezoneAbbr STRING,
  timezoneAbbrDst STRING,
  dayId INT,
  runId INT
)
USING PARQUET
PARTITIONED BY (dayId, runId)
LOCATION '/mnt/lake/Landing/Exchanges';

MSCK REPAIR TABLE $landing_database_name.DimExchange;
```
All Landing tables are partitioned by *dayId* and *runId*.

In the Staging layer some tables are partitioned by a business key and some tables are not partitioned at all. In the example below FactEod table is partitioned by Date key:

![image](https://user-images.githubusercontent.com/58121577/181737255-5b23f46f-63a3-4c71-ac07-68d25012c765.png)

All Staging tables are in Delta Lake format and are also defined in [SQL DDL](https://github.com/SenYaEn/fin_data_databricks/tree/main/fin_data/staging_ddl):

```sql
DROP TABLE IF EXISTS staging.FactEod;

CREATE TABLE staging.FactEod (
    PK STRING
  , Open FLOAT
  , High FLOAT
  , Low FLOAT
  , Close FLOAT
  , Volume FLOAT
  , SplitFactor FLOAT
  , Dividend FLOAT
  , Symbol STRING
  , ExchangeId STRING
  , `Date` INT
)
USING DELTA
PARTITIONED BY (`Date`)
LOCATION '/mnt/lake/Staging/FactEod'
```

Function which performs the merge between Landing and Staging tables is:

```merge_into_delta_table``` : merges records from source table into a Delta table

The load can be either a *full* one or *non-full*. In the full load (```is_full_reload=True```) data is loaded from Landing views (e.g. ```landing.vw_FactEod_distinct ```) where only unique rows are filtered out. In the non-full load (```is_full_reload=False```) only the most recent dayId and runId from a corresponding Landing table are selected (within runId partition all records are unique).

Other functions involved in loading data into Landing and Staging tables are:

```get_table_location``` : retrieves Hive table location and its directory (which is a subset of a full location)

```get_max_day_and_run_id``` : retrieves max dayId partition and its corresponding max runId

```get_next_run_id``` : determines the next runId for a given day

```get_companies_per_exchange``` : gets a list of all the companies associated with a given stock exchange(s)

```list_to_string``` : converts list into a string

```int_to_date``` : converts date in integer format into date

```get_min_max_date``` : first determines a max date for each company and then takes the min date from that set

```repair_table``` : runs MSCK REPAIR TABLE on a partitioned table

```run_ddl_notebook``` : runs a notebook at a specified path

```write_aip_to_staging_dated_fact_tables``` : makes API calls, writes data to Data Lake, and merges data into Delta staging table for Fact tables that have the same API path structure

```write_aip_to_staging_dim_tables``` : makes API calls, writes data to Data Lake, and merges data into Delta staging table for Dim tables that have the same or similar API path structure

```update_dim_tables``` : retrieves parameters for Dim tables from YAML items and runs a data extract from API call to merge into Delta staging table

```update_fact_tables``` : retrieves parameters for Fact tables from YAML items and runs a data extract from API call to merge into Delta staging table

```update_tables``` : retrieves general ETL parameters and then determines if Dim or Fact function should be executed

The *Analytics* layer is composed of Hive views built on top of Staging tables. Views are also defined using [SQL DDL](https://github.com/SenYaEn/fin_data_databricks/tree/main/fin_data/analytics_ddl):

```sql
DROP VIEW IF EXISTS analytics.vw_DimExchange;

CREATE VIEW analytics.vw_DimExchange AS
(
  SELECT ExchangeId
       , Name
       , Acronym
       , Country
       , CountryCode
       , City
       , Website
       , TimezoneName
       , TimezoneCode
       , CurrencyId
  FROM staging.DimExchange
)
```

## Conceptual Design
This section covers the conceptual design of the Staging layer as all the dables in this layer are of Delta Lake type which has the strongest Schema Validation built in comparing to other types of tables.

#### FactEod relations

![image](https://user-images.githubusercontent.com/58121577/181748350-81ed4999-828f-4945-a36a-53748b85d67c.png)


***.....the rest of the documentation is being worked on and will be added soon...***
