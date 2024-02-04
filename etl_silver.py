from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from google.oauth2 import service_account
import pandas as pd
import json

def extract_bronze_data():
    '''
    extract the unprocessed data from bronze layer
    '''

    project_id = "innate-plexus-409501"
    db_id = "db_iowa_liquorsales_bronze"
    tb_id = "tb_sales_history"

    bronze_data = spark.read.format('bigquery') \
        .option("parentProject", project_id) \
        .option("table", project_id + "." + db_id + "." + tb_id) \
        .load()
    
    return bronze_data

def silver_data_process(bronze_data):
    '''
    process extracted data to parquet format using pyspark
    '''

    # Processing and filtering the raw data provided by the bronze layer
    silver_data = bronze_data \
    .withColumn("date", to_date(substring(col('date'), 1, 10), "yyyy-MM-dd"))\
    .withColumn("sale_bottles", col('sale_bottles').cast('int')) \
    .withColumn("sale_dollars", round(col('sale_dollars').cast('double'), 2)) \
    .withColumn("sale_liters", round(col('sale_liters').cast('double'), 2)) \
    .withColumn("invoice_line_no", col('invoice_line_no').cast('string')) \
    .withColumn("store", col('store').cast('string')) \
    .withColumn("city", col('city').cast('string')) \
    .withColumn("county", col('county').cast('string')) \
    .withColumn("category", col('category').cast('string')) \
    .withColumn("category_name", col('category_name').cast('string')) \
    .withColumn("itemno", col('itemno').cast('string')) \
    .withColumn("im_desc", col('im_desc').cast('string')) \
    .withColumn("date", col('date').cast('string')) \
    .withColumnRenamed("date", "sale_date") \
    .filter(
        (col('sale_bottles') > 0) & 
        (col('sale_dollars') > 0) & 
        (col('sale_liters') > 0) &
        (col('invoice_line_no').isNotNull()) &
        (col('store').isNotNull()) &
        (col('city').isNotNull()) &
        (col('county').isNotNull()) &
        (col('category').isNotNull()) &
        (col('category_name').isNotNull()) &
        (col('itemno').isNotNull()) &
        (col('im_desc').isNotNull())
    )

    # Write data in parquet format
    silver_data.write.parquet('silver_processed_data.parquet', mode="overwrite")

    # Stop the Spark session
    spark.stop()

def load_data(db_layer, tb_id):
    '''
    loading processed data to silver layer
    '''

    # Set BigQuery parameters
    db_id = 'db_iowa_liquorsales_' + db_layer
    service_account_info = json.load(open('GBQ.json'))
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info)

    df = pd.read_parquet("silver_processed_data.parquet")
    df.to_gbq(credentials = credentials, destination_table = db_id + "." + tb_id, if_exists = 'replace')


if __name__ == "__main__":

    # Executing the pipeline

    # Create a Spark session
    spark = SparkSession.builder \
        .appName("Iowa") \
        .config("spark.executor.memory", "6g") \
        .config("spark.driver.memory", "6g") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.35.1") \
        .config("spark.hadoop.fs.gs.project.id", "innate-plexus-409501") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "GBQ.json") \
        .getOrCreate()
    
    db_layer = "silver"
    tb_id = "tb_sales_history"
    
    silver_data_process(extract_bronze_data())
    load_data(db_layer, tb_id)