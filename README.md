# BigQuery Data Analysis And Engineering - ETL and BI Project

This project was developed to create a solution that extracts, using an API, data from the Iowa state, in USA, regarding the historical sales of liquor, transforming it using PySpark in order to create a DW using BigQuery, which will provide filtered, transformed and refined information which the business team can consume and create BI reports using Power BI, providing analytical insights and report status regarding sales.

Below, the ETL Diagram showing the solution as a whole:

![image](https://github.com/T1burski/BQ-Data-Analysis-And-Engineering/assets/100734219/0fe72888-cc9c-4c0b-942f-3b3a55d3c81e)

The medallion architecture was adopted in order to have a data design pattern, maintaining organization and data quality to high standards.

## 1) The Business Problem
As stated before, we were given the challenge of extracting the data from the Iowa State database, load it in our cloud data warehouse using GCP (BigQuery), transform the data to meet business requirements and develop BI reports using Power BI. With this requested information reaching the business users of our team through clean data and insightful data visualization, we can thrive as a data driven organization. More precisely, the information requested by the team is related to:


- Total Revenue in the last 5 months;
- Total invoices made in the same period;
- Total volume (L) sold in the same period;
- Total revenue per Region and top product categories regarding revenue in the same peridod;
- Top stores regarding invoices made and the revenue per invoice in the top stores in the same peridod;
- Time series of the revenue in the same period.


## 2) Architecture and Data Engineering
In order to be cost effective, GCP BigQuery was chosen as the cloud DW. Also, following best practices, the medallion architecture was adopted. Below, we will describe in details each layer of the data architecture, explaining their objectives and pipeline (inputs and outputs).

### 2.1) Bronze Layer: Raw Data
This layer's data is ingested and built using the etl_bronze.py script. It receives the data from Iowa database using a SODA API

