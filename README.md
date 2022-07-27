# fin_data_databricks: Market data ETL

This repository serves for extraction, transformation, and loading Marketstack API data into a set of fact and dimention tables on Azure Data Lake for further analysis by Power BI.

Marketstack API documentation can be found [here](https://marketstack.com/documentation)

## Software
The solution in this repo runs within the Azure platform and uses the following resources:
1. Databricks
2. Data Lake
3. Key Vault

To perform API calls to Marketstack platform an API key is required. More information on how to obtain the key can be found on the [Marketstack website](https://marketstack.com).

### Databricks
A standard Cluster will suffice to run the solution and can be scaled up depending on the processing speed requirements. For a demo run the following cluster type was used:

![image](https://user-images.githubusercontent.com/58121577/181119944-7d6ad725-8266-4928-a8b8-cb3b30ea064e.png)

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

## Architecture
Architecture of the solution in this repo is best described using the Extract, Transform, and Load (ETL) framework. All Python functions involved in the process are located in the [functions_master.py](https://github.com/SenYaEn/fin_data_databricks/blob/main/fin_data/functions_master.py) file.

### Extraction

Data is extracted from the Marketstack platform by performing API calls. Functions directly involved in creating and performing API calls are the following (more detailed documentation for each function is attached to each function definition within the code):

```get_total_api_count``` : gets the total number of records in the whole API response (not in a page, the whole response)

```get_limit_and_offset``` : determines the necessary limit and offset for an API request to ensure all requested data is loaded

```get_basic_api_json``` : obtains all the records from the API response on the current page


***.....rest of the documentation is being worked on and will be added at a later date...***
