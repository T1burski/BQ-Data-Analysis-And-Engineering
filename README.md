# BigQuery Data Analysis And Engineering - ETL and BI Project

This project was developed to create a solution that extracts, using an API, data from the Iowa state, in USA, regarding the historical sales of liquor, transforming it using PySpark in order to create a DW using BigQuery, which will provide filtered, transformed and refined information which the business team can consume and create BI reports using Power BI, providing analytical insights and report status regarding sales.

Below, the ETL Diagram showing the solution as a whole:

![image](https://github.com/T1burski/BQ-Data-Analysis-And-Engineering/assets/100734219/9dc9751a-9a74-432f-b78e-97fc24caefa6)

The medallion architecture was adopted in order to have a data design pattern, maintaining organization and data quality to high standards.

Hadoop Version: 3.3.5

PySpark Version: 3.5.0

google-cloud-bigquery Version: 3.14.1

google-cloud-bigquery-storage Version: 2.24.0

Pandas GBQ Version: 0.20.0

## 1) The Business Problem
As stated before, we were given the challenge of extracting the data from the Iowa State database, load it in our cloud data warehouse using GCP (BigQuery), transform the data to meet business requirements and develop BI reports using Power BI. With this requested information reaching the business users of our team through clean data and insightful data visualization, we can thrive as a data driven organization. More precisely, the information requested by the team is related to:


- Total Revenue in the last 5 months;
- Total invoices made in the same period;
- Total volume (L) sold in the same period;
- Total revenue per Region and top product categories regarding revenue in the same peridod;
- Top stores regarding invoices made and the revenue per invoice in the top stores in the same peridod;
- Time series of the revenue in the same period.


## 2) Architecture and Data Engineering
In order to be cost effective, GCP BigQuery was chosen as the cloud DW. Also, following best practices, the medallion architecture was adopted. Below, we will describe in details each layer of the data architecture, explaining their objectives and pipeline (inputs and outputs). The GBQ.json file used in the scripts contains our credentials to GCP BigQuery.

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
This layer's data is ingested from the silver layer and built with the script named etl_gold.py. The table loaded in this layer is adjusted to answer the business needs regarding information availability and is ready for user consumption (with BI tools such as Power BI, for example). In order to provide the necessary data structure, the following ETL process is performed: extract the tb_sales_history and the tb_county_regions tables from the silver layer, selecting only the desired columns from the tb_sales_history table and finally joining both tables using the county as a key in order to bring into the sales history the region of each county.


## 3) The Pipeline
The scripts must be executed in a specific order so that, with each data update, the ETL processes and layers do not break and the right updated data flows in the right sequence, maintaining data quality in each table. With that being said, the scripts must be executed in the following order:

### etl_bronze.py -> etl_silver.py -> etl_gold.py

while the etl_silver_regions.py script only runs once to populate the associated table with the static data needed.


## 4) Google BigQuery: Checking and Analyzing the Data
Below, we can see that our BigQuery DW is fully populated with the data mentioned before:

![image](https://github.com/T1burski/BQ-Data-Analysis-And-Engineering/assets/100734219/9d14b4c2-faea-4780-b143-03af464d653a)

Let's try to build a SQL query to check out data in the gold layer:

![image](https://github.com/T1burski/BQ-Data-Analysis-And-Engineering/assets/100734219/d1bb0d29-8ab3-4034-a066-901c10029cf2)

Nice! Now, let's build a little more complex query:

![image](https://github.com/T1burski/BQ-Data-Analysis-And-Engineering/assets/100734219/c751cba5-4ae7-4830-8f90-f432e1a5882d)

Here, we calculated the total liquor volume and revenue per region, year and month.


## 5) Business Intelligence: Power BI Connection
We have our DW set up and ready for business use. Power BI has a connection type available for Google BigQuery. After accessing it, providing the necessary credentials, we can select our tables as shown below:

![image](https://github.com/T1burski/BQ-Data-Analysis-And-Engineering/assets/100734219/e7d56666-3c73-404b-8e5c-f2161ddf1179)

Now, we develop the Power BI report, which will deliver, in a simple and fast way, the information our users requested, such as a time series of the revenue, the product categories with the highest revenues, the total revenue per Region and the stores that made the most sales regarding the number of invoices, also showing how much was the revenue per invoice, everything regarding the last 5 months of sales, not including the current month.

The users also requested a way to export to Excel the data in tabular format. So, the Power BI with the mentioned features was created, and below we can see screenshots of the pages:

![image](https://github.com/T1burski/BQ-Data-Analysis-And-Engineering/assets/100734219/e33762bf-bd13-4629-998b-668ffbf92b12)

![image](https://github.com/T1burski/BQ-Data-Analysis-And-Engineering/assets/100734219/cb9e37bd-d28e-40b5-9ecf-2be7686ef80f)

In case the user wants to visualize the revenue in time considering a monthly granularity, they can use the drill-up functionality within the visual, resulting in:

![image](https://github.com/T1burski/BQ-Data-Analysis-And-Engineering/assets/100734219/71ca0a44-916b-49d7-a5d5-5867a22a8ea5)


## 6) Conclusion and Future Additions:
This project had, as its main goal, to develop an ETL process showcasing the usage of API calls, PySpark, cloud service (GCP BigQuery) and Business Intelligence in order to solve a company's data and information necessity. There are more tools we could have used to guarantee even more robustness to the solution implemented, but for now, the simplicity we tackled solves nicely the company's needs.

Now, some future additions we wish to implement in the near future: The usage of Github Actions or Apache Airflow to automatically run the data pipeline every month start, providing up do date information for the business without the need of our intervention. 
