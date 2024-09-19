import os
import logging
from dotenv import load_dotenv
from pyspark.sql import SparkSession


class Util:
    def __init__(self):
        current_directory = os.getcwd()
        print(current_directory)
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def get_data_directory(self):
        # os.dir()
        pass

    def source_url_getter(self):
        url = "http://www.floatrates.com/daily/usd.json"
        return url

    def spark_session_creator(self):
        """Create a Spark Session"""
        load_dotenv()
        try:
            spark = SparkSession.builder \
                .appName("CurrencyRate_And_PostSalesAnalysis") \
                .config("spark.driver.extraJavaOptions", "--add-opens java.base/java.nio=ALL-UNNAMED") \
                .getOrCreate()
            logging.info(f"Spark Session started successfully: {spark} ")
        except Exception as e:
            logging.error(f"An error occurred while creating the Spark session: {e}")
            print(f"An error occurred while creating the Spark session: {e}")
        return spark



    #Variables shoudl be nouns
    #Function & Method shoudl be verbs


if __name__ == '__main__':
    u = Util()
    # u.get_data_directory()
    u.spark_session_creator()
