from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from google.oauth2 import service_account
import pandas as pd
import json

def extract_csv_regions():
    '''
    extract the csv file containing the static relation
    between counties and regions in Iowa
    '''
    regions = spark.read.option("header", True).csv("counties_iowa_regions.csv")
    
    return regions

def regions_data_process(regions):
    '''
    process extracted data to parquet format using pyspark
    '''

    # Processing and filtering the raw data provided by the bronze layer
    regions_treated = regions \
    .filter((col('County').isNotNull()))

    # Write data in parquet format
    regions_treated.write.parquet('regions_processed_data.parquet', mode="overwrite")

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

    df = pd.read_parquet("regions_processed_data.parquet")
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
    tb_id = "tb_county_regions"
    
    regions_data_process(extract_csv_regions())
    load_data(db_layer, tb_id)