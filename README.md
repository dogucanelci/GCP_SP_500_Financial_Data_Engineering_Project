![alt text](images/image.png)

<h1 style="display: inline-block;"> S&P 500 Financial Data End to End Data Engineering Project on Google Cloud Platform Orchestrated by Google Composer </h1>

## What Is the S&P 500 Index?
The S&P 500 Index, or Standard & Poor's 500 Index, is a market-capitalization-weighted index of 500 leading publicly traded companies in the U.S. The index actually has 503 components because three of them have two share classes listed.
[S&P 500 component stocks](https://en.wikipedia.org/wiki/List_of_S%26P_500_companies)
## yfinance
yfinance is a Python library which offers a threaded and Pythonic way to download market data from Yahoo Finance.
[Documentation](https://pypi.org/project/yfinance/)
## Dataset
The dataset was created with data extracted from the 2 sources mentioned above (wiki, yfinance).
This created Dataset contains balance sheet information such as net profit, total income, gross expense for each quarter and close price values of the last 7 days, as numerical data, through the yfinance library of all companies in the SP500 index. As descriptive information, it includes the company's name, symbol, year of establishment, place of establishment and sector information.

Preview of raw dataset:
![alt text](image.png)


## 📝 Table of Contents
1. [Project Overview](#introduction) <br>
2. [Project and DAG Architecture](#project_architecture) <br>
  2.1. [Web Scrapper Building(Data Extraction)](#data_extraction) <br>
  2.2. [Data Ingestion](#data_ingestion) <br>
  2.3. [Data Loading](#data_loading) <br>
  2.4. [Data Modeling](#data_modeling) <br>
  2.5. [Data Transformation](#data_transform) <br>
  2.5. [System Migration](#system_migration) <br>
  2.6. [Data Reporting](#data_reporting) <br>
3. [Credits](#credits) <br>
4. [Contact](#contact) <br>

<a name="introduction"></a>
## 🔬 Project Overview 

This project is an end-to-end Data Engineering project orchestrated by Google Cloud Composer, using multiple technologies, and applied on the Google Cloud platform. S&P 500 Financial Dataset is created for this project by extract web data from many sources. <br>
In first step, Astro CLI project is created for testing whole process in local first.
In next step, a Web Scrapper which extract data from multiple sources is built in Python. After this step,The raw dataset which include financial metrics like total revenue,net profit etc. and descriptive fields as company name,foundation date,sector etc. and saved as .csv file into composer storage bucket.<br>
In next step, created raw dataset is pushed into Storage Bucket.<br>
In next step, Raw Dataset is loaded into BigQuery DataWarehouse Platform. Then Raw Data is investigated and Data Modeling is applied by created stored procedures in BigQuery dataset. In this part, fact and dim tables are created into BigQuery Data Warehouse. <br>
In next step, top layer, multi dimensional tables are created and last part, S&P 500 Financial Report is created in Google Looker Studio by using top layer transformed data.
When the whole ETL pipeline is done perfectly, the DAG is migrated from local(Astro CLI project) to Google Cloud Composer Environment.
Each step which are described above represents a task in SP_500 DAG. You can see DAG structure and all tasks in detail in below.<br>
<a name="project-architecture"></a>
## 📝 Project Architecture

You can find the detailed information on the diagram below:

![alt text](images/project_structure.jpg)

---------------------------


Retail Dag Architecture:

![alt text](images/dag_structure.png)

---------------------------

<a name="data_extraction"></a>
### 📤 Web Scrapper Building(Data Extraction)
- A basic Web Scrapper script is implemented by using yfinance,beautifulsoup, requests and pandas libraries. 
Steps are:
  1 - S&P 500 Companies and their descriptive information is extracted from wiki page.(Sector,Sub-Industry,Company Name,Symbol,Foundation Date,State,Province)
  2 - All Financial metrics of companies are extracted by yfinance.(Net Income,Total Revenue,Gross Profit,Revenue Cost for each Quarter)
  3 - Last 7 days Close Prices of the companies are extracted by yfinance.
  4 - All these information is merged into one table and the table is stored as csv file which is described as 'Raw Dataset'.



  
![alt text](images/image-1.png)

<a name="data_ingestion"></a>
### 📤 Data Ingestion
- sp_500.py file is created for SP500 DAG implementation.
- Raw dataset is pushed to Google Bucket Storage from Google Composer DAG Bucket by new task implementation in SP500 DAG as shown below:

![alt text](images/image-2.png)
  

<a name="data_loading"></a>
### ⚙️ Data Loading
- Raw data pushed into Bucket Storage is loaded into Google BigQuery Data Warehouse by implementing new 2 task in SP500 DAG.
- First task create an empty table with defined schema informations.
- Second task apply loading process.

![alt text](images/image-3.png)
![alt text](images/image-4.png)


<a name="data_modeling"></a>
### 📊 Data Modeling
- Data Modeling design is done and transformation scripts are implemented which is used by created Stored Procedures to create new dimensional model in BigQuery.

![alt text](images/image-6.png)

![alt text](images/image-7.png)
  

<a name="data_transform"></a>
### 📊 Data Transformation
- Stored Procedures are created in BigQuery side for each table in data model to create and overwrite insert spesific data for them.
- DAG tasks (BigQueryInsertJobOperator) are created in SP500 DAG which is designed as execute in paralel.

![alt text](images/image-10.png)

![alt text](images/image-8.png)


<a name="system_migration"></a>
### 📊 System Migration
- All the ETL process implemented and tested in local by Astro CLI is now ready to run completely in Google Cloud Platform. So, further steps are:
- Google Composer environment is created.
- Requirements are installed into Composer VM instance.
- SP500.py (DAG and tasks file)
and dag_test1.py (used for data extraction and raw dataset creation) is inserted into Composer DAG storage bucket.
- Connection id information which is located in Composer Airflow UI connection settings are implemented in DAG file.
- SP500 DAG is trigerred manually in Airflow UI. 

![alt text](images/image-11.png)


<a name="data-reporting"></a>
### 📊 Data Reporting
- BigQuery is connected with Looker Studio BI , and used the Views of the DB to create interactive and insightful data visualizations.

![alt text](images/image-14.png)


### 🛠️ Technologies Used

- **Data Source**: Google Cloud Bucket Storage
- **Orchestration**: Google Cloud Composer / Airflow
- **ETL Process**: Python
- **Transformation**: BigQuery Stored Procedures
- **Data Testine**: Astro CLI
- **Date Warehousing**: Bigquery, t-SQL
- **Storage**: Google Cloud Bucket Storage
- **Data Visualization**: Looker Studio

<a name="credits"></a>
## 📋 Credits

- This Project is designed and implemented by Doğu Can Elçi from zero to end.

<a name="contact"></a>
## 📨 Contact Me

[LinkedIn](https://www.linkedin.com/in/elcidogucan/)
[Website](https://www.dogucanelci.com)
[Gmail](dogucanelci@gmail.com)
