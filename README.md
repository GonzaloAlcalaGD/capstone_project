# Capstone Project - Big Data Engineering Internship -
[![PyPI version](https://badge.fury.io/py/apache-airflow.svg)](https://badge.fury.io/py/apache-airflow)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/apache-airflow.svg)](https://pypi.org/project/apache-airflow/)

**Table of contents**
- [Project description](#project-description)
    - [Stages](#stages)
    - [Aggregations](#aggregations)
    - [Think about](#think-about)
    - [Concepts](#concepts)
    - [Optional](#optional)
    - [Deliverable](#deliverable)
- [Requirements](#requirements)

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

### Stages:

1. Create data generators : Create data using the 3 formats. Create around 100 entries for each format and for a total of 3 days. 
2. Integrate tools: You can use RDBMS (Postresql, or MySQL) + Spark locally, or you can also choose Docker Desktop or Cloud. Once tools are installed and can communicate please perform the following aggregations in the Aggregations section, and save in the target folder (simulating the Data Warehouse). 
3. Install Airflow or integrate in Docker / Cloud, then schedule the Spark execution to Aggregate the data of the first day in one hour, next day the next hour, last day the following hour. 
4. Install HDFS or use Docker / Cloud and configure spark jobs to save the resulting data there. 

### Aggregations:
 
Build next tables:
1. Count and sum amount transactions for each type (online or offline(in store)) for day
2. Count and sum amount transactions for each store for day
3. Count and sum amount transactions for each city (city can be extracted from address) for day

### Think about:
**Is it convenient to ingest all fields to the data warehouse?**

**Is the source data going to be there forever? What about the data in DWH?**
 
**What do you do if you have a transaction that is present in more than one format ?** 

**What happens If you have data in the DWH for 2 years and you have a request from the client to calculate a new attribute/aggregation?**

### Concepts:
    - PII (personal identifiable information) 
    - TTL (time to live) Retention policy 1 yrs 
    - Duplicates (remover / take last /aggregate both / etc) 
    - Backfill 
    - Null policy 

### Optional:
    - Add a few duplicates and apply 3 techniques: Drop duplicates, take latest, add count column 
    - Create a fourth dataset with customers data and enrich the unified dataset with Customer information where needed.

### Deliverable:
    - Create a personal github project. Add readme with detailed description of the project. 

## Requirements
Capstone project is tested with:
|                     | Main version (dev)           | Stable version               |
|---------------------|------------------------------|------------------------------|
| Python              | 3.7.14                       | 3.7.14                       |
| Docker desktop      | 4.10.                        | 4.10.1                       |
| PostgreSQL          | 13                           | 13                           |
| Airflow             | 2.4.1                        | 2.4.1                        |
| Redis               | Latest (7.0)                 | Latest (7.0)                 |
| Celery              | v5.2.7 (dawn-chorus)         | v5.2.7 (dawn-chorus)         |
| HDFS                | 3.2.1                        | 3.2.1                        |
| Java                | 1.8.0_341                    | 1.8.0_341                    |

**Docker dekstop resources**
For proper operation it is recommended to configure the advanced Docker resources as described below: 
- CPUs : 6
- Memory : 12.00 GB
- Swap : 1 GB
- Disk image size: 72GB

## Geting started
In order to execute the project you need to have the following tools installed in your local machine.
1. Docker desktop(https://www.docker.com/products/docker-desktop/)