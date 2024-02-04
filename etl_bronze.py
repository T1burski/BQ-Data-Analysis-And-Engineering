import requests
import json
from datetime import timedelta, date
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from google.oauth2 import service_account
import pandas as pd

def extract_api_data():
    '''
    extract the raw data using SODA API
    '''

    def fetch_all_data(api_url):
        all_data = []

        while api_url:
            # Make a request to the API
            response = requests.get(api_url)

            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                # Parse JSON data
                data = response.json()
                all_data.extend(data)

                api_url = response.links.get('next', {}).get('url')
            else:
                print(f"Error: Unable to fetch data. Status code: {response.status_code}")
                break

        return all_data


    # Creating variable to store the first day of 5 months ago
    current_date = date.today()
    date_7_months_ago = current_date - relativedelta(months=5)
    first_day_next_month = date_7_months_ago.replace(day=1)

    # Creating variable to store the last day of the last month
    date_1_month_ago = current_date.replace(day=1)
    last_day_1_month_ago = date_1_month_ago - timedelta(days=1)

    # API endpoint
    api_url = "https://data.iowa.gov/resource/m3tr-qhgy.json?$limit=10000000&$order=date&$where=date >= '" + str(first_day_next_month) + "' AND date <= '" + str(last_day_1_month_ago) + "'"

    # Fetch all data
    all_data = fetch_all_data(api_url)

    # Save all data to a file
    with open("iowa_data.json", "w") as json_file:
        json.dump(all_data, json_file)

def raw_data_parquet():
    '''
    process extracted data to parquet format using pyspark
    '''

    # Create a Spark session
    spark = SparkSession.builder \
        .appName("Iowa") \
        .config("spark.executor.memory", "6g") \
        .config("spark.driver.memory", "6g") \
        .getOrCreate()

    # Load data from the JSON file
    data = spark.read.json("iowa_data.json")

    # Selecting the necessary columns
    needed_cols = [
        "invoice_line_no"
        , "date"
        , "store"
        , "city"
        , "county"
        , "category"
        , "category_name"
        , "itemno"
        , "im_desc"
        , "sale_bottles"
        , "sale_dollars"
        , "sale_liters"
    ]

    raw_data = data.select(*needed_cols)

    # Write data in parquet format
    raw_data.write.parquet('extracted_data.parquet', mode="overwrite")

    # Stop the Spark session
    spark.stop()

def load_data(db_layer, tb_id):
    '''
    loading unprocessed data to bronze layer
    '''

    # Set BigQuery parameters
    db_id = 'db_iowa_liquorsales_' + db_layer
    service_account_info = json.load(open('GBQ.json'))
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info)

    df = pd.read_parquet("extracted_data.parquet")
    df.to_gbq(credentials = credentials, destination_table = db_id + "." + tb_id, if_exists = 'replace')


if __name__ == "__main__":

    # Executing the pipeline

    db_layer = "bronze"
    tb_id = "tb_sales_history"
    
    extract_api_data()
    raw_data_parquet()
    load_data(db_layer, tb_id)