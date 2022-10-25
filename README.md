# Capstone Project - Big Data Engineering Internship -
[![PyPI version](https://badge.fury.io/py/apache-airflow.svg)](https://badge.fury.io/py/apache-airflow)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/apache-airflow.svg)](https://pypi.org/project/apache-airflow/)


## Project description

As a Data Engineer you will need to design and implement some part of the data platform for a data analytics team. Customers have various sources but data is not unified, so we also need to think about data structure. We have 3 sources of data that need to be ingested. These sources are updated every day around 11PM. 
 
We have following sources:
RDBMS (Postresql, or MySQL)
Parquet files
Json files

1. RDBMS tables:
    - Transaction
	    - id int,
	    - Customer_id int,
	    - Transaction_ts timestamp,
	    - Amount int
    - Customer
	    - Id int,
	    - First_name int,
	    - Last_name int,
	    - Phone_number int,
	    - Address string

2. JSON structure:
    - {
        ‘id’:1,
        “ts”: 2022--06-06T22:06:06, 
        “Customer_first_name” : “test”
        “Customer_last_name”: “test”,
        “Amount”: 1000
        “Type” : “0” # 0 - in_store, 1-online	
        }

3. Parquet structure:
	- Id: int 
	- Customer: Struct
		- First_name: String
		- Last_name: String
	- Amount: int
	- ts: timestamp,
	- Store_id: int

Stages:

Create data generators : Create data using the 3 formats. Create around 100 entries for each format and for a total of 3 days. 
Modificar id de los json y parquet para que sean únicos al igual que las inserciones a las tablas 
Integrate tools: You can use RDBMS (Postresql, or MySQL) + Spark locally, or you can also choose Docker Desktop or Cloud. Once tools are installed and can communicate please perform the following aggregations in the Aggregations section, and save in the target folder (simulating the Data Warehouse). 
Install Airflow or integrate in Docker / Cloud, then schedule the Spark execution to Aggregate the data of the first day in one hour, next day the next hour, last day the following hour. 
Install HDFS or use Docker / Cloud and configure spark jobs to save the resulting data there. 

Aggregations:
 
Build next tables:
Count and sum amount transactions for each type (online or offline(in store)) for day
Count and sum amount transactions for each store for day
Count and sum amount transactions for each city (city can be extracted from address) for day

Think about:
Is it convenient to ingest all fields to the data warehouse? 
Fields like personal information are not necessary for the data warehouse as they don’t represent quantitative information for the analytics the data science team would perform.

Is the source data going to be there forever? What about the data in DWH?
Since the data will only be generating data for a total of 3 days our data ingestion should stop at the same date. 
Regarding the data in the warehouse we can store hot data in cloud storage and old data in buckets that don't get access too often.
 
What do you do if you have a transaction that is present in more than one format ? 
Only ingest the transaction data that has the latest timestamp.

What happens If you have data in the DWH for 2 years and you have a request from the client to calculate a new attribute/aggregation? 
We can retrieve the data from the storage and perform the aggregation 
Concepts:
PII (personal identifiable information) 
TTL (time to live) Retention policy 1 yrs 
Duplicates (remover / take last /aggregate both / etc) 
Backfill 
Null policy 

Optional:
Add a few duplicates and apply 3 techniques: Drop duplicates, take latest, add count column 
Create a fourth dataset with customers data and enrich the unified dataset with Customer information where needed.

Deliverable:
Create a personal github project. Add readme with detailed description of the project. 

