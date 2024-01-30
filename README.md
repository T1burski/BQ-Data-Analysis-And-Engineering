# BigQuery Data Analysis And Engineering - ETL and BI Project

This project was developed to create a solution that extracts, using an API, data from the Iowa state, in USA, regarding the historical sales of liquor, transforming it using PySpark in order to create a DW using BigQuery, which will provide filtered, transformed and refined information which the business team can consume and create BI reports using Power BI, providing analytical insights and report status regarding sales.

Below, the ETL Diagram showing the solution as a whole:

![image](https://github.com/T1burski/BQ-Data-Analysis-And-Engineering/assets/100734219/9dc9751a-9a74-432f-b78e-97fc24caefa6)

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
This layer's data is ingested and built using the etl_bronze.py script. It receives the data from Iowa database using a SODA API adapted to only extract invoices made in the last 5 months (temporal threshold determined by the business) using a .json file which is converted to parquet using PySpark. Also, in this step of the ETL pipeline, we select the desired columns from the Iowa database:

"invoice_line_no"

"date"

"store"

"city"

"county"

"category"

"category_name"

"itemno"

"im_desc"

"sale_bottles"

"sale_dollars"

"sale_liters"

The data is then loaded to our BigQuery DW using the db: db_iowa_liquorsales_bronze, naming the table as tb_sales_history. This loading process of the resulting parquet is done using "to_gbq" method. In the end, this layer (bronze) exists to store the data in a raw format, without any transformations like filtering and casting.

### 2.2) Silver Layer: Filtered and Treated Data
This layer's data is ingested from the bronze layer. There are two scripts that create one table each in this layer: etl_silver.py and etl_silver_regions.py. The first one processes the data from the table "db_iowa_liquorsales_bronze.tb_sales_history", applying casting and filtering actions in order to result in a clean table, making sure that eventual data flaws in the original source are not propagated into our BigQuery DW. The second script makes the one-time ingestion of a .csv table that contains the relation between Counties and their respective Regions in the state of Iowa. This is a static file since these relations are not meant to change overtime. In the end, this layer is represented by the db: db_iowa_liquorsales_silver, which will contain the two mentioned tables named as: tb_sales_history and tb_county_regions. In the end, this layer (silver) exists to store processed and filtered that ready to be processed and generate final tables which can be used by the final business user.

### 2.3) Gold Layer: Business Level and Consumption Ready Data




