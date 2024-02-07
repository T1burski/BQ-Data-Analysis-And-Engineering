from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from google.oauth2 import service_account
import pandas as pd
import json

def extract_silver_data():
    '''
    extract the processed data from silver layer
    '''

    project_id = "innate-plexus-409501"
    db_id = "db_iowa_liquorsales_silver"
    tb_id_history = "tb_sales_history"
    tb_id_regions = "tb_county_regions"

    silver_data_history = spark.read.format('bigquery') \
        .option("parentProject", project_id) \
        .option("table", project_id + "." + db_id + "." + tb_id_history) \
        .load()
    
    silver_data_regions = spark.read.format('bigquery') \
        .option("parentProject", project_id) \
        .option("table", project_id + "." + db_id + "." + tb_id_regions) \
        .load()
    
    return silver_data_history, silver_data_regions

def gold_data_process(silver_data_history, silver_data_regions):
    '''
    process extracted data to desired gold format using pyspark
    for business consuption
    '''

    # Processing and filtering the raw data provided by the bronze layer
    gold_data_history = silver_data_history \
    .select(["invoice_line_no", "sale_date", "store", "city", "county", "category_name", "sale_bottles", "sale_dollars", "sale_liters"])

    cond_regions = [silver_data_history.county == silver_data_regions.County]

    gold_data_history = gold_data_history.join(silver_data_regions, cond_regions, 'left') \
                                         .select(gold_data_history["*"], silver_data_regions["Region"])

    # Write data in parquet format
    gold_data_history.write.parquet('gold_processed_data.parquet', mode="overwrite")

    # Stop the Spark session
    spark.stop()

def load_data(db_layer, tb_id):
    '''
    loading processed data to gold layer
    '''
    
    # Set BigQuery parameters
    db_id = 'db_iowa_liquorsales_' + db_layer
    service_account_info = json.load(open('GBQ.json'))
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info)

    df = pd.read_parquet("gold_processed_data.parquet")
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
    
    db_layer = "gold"
    tb_id = "tb_sales_history_processed"
    
    gold_data_process(extract_silver_data()[0], extract_silver_data()[1])
    load_data(db_layer, tb_id)
