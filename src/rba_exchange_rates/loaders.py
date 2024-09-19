from pyspark.sql import SparkSession
from transformers import Transformer
from pyspark.sql.utils import AnalysisException
import logging
import csv


class Loader:
    def __init__(self, spark):
        self.spark = spark
        # transformer = Transformer()
        # self.formatter_sp_df = transformer.formatted_sp_dataframe()
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        if self.spark is None:
            raise RuntimeError("Spark session is not available")
        else:
            logging.info(f"Spark Session started successfully: {self.spark} ")

    def csv_rdd_loader(self):
        """CSV Headers"""
        csv_headers = ['incident_id', 'incident_type', 'vin_number', 'make', 'model', 'year', 'incident_date', 'description']
        # Read a text file and create an RDD using SparkContext
        # rdd = self.spark.sparkContext.textFile("/Users/kiranchavadi/Documents/KarthikNewAssignments/DE2024/May_2024/post_sales_analysis/data/files/data.csv")
        df = self.spark.read.csv(
            "/Users/kiranchavadi/Documents/KarthikNewAssignments/DE2024/May_2024/post_sales_analysis/data/files/data.csv",
            header=False, inferSchema=True) #Read CSV file into DataFrame
        df = df.toDF(*csv_headers)    # Rename the columns of the DataFrame using csv_headers
        rdd = df.rdd  # Convert DataFrame to RDD
        # sample = rdd.take(5)  # Take the first 5 elements
        # logging.info(f"Sample of RDD: {type(sample), sample} ")    # (<class 'list'>, [Row(incident_id =1, incident_type='I', vin_number='VXIO456XLBB630221', make='Nissan', model='Altima', year=2003, Incident_date=datetime.date(2002, 5, 8), Description='Initial sales from TechMotors'),

        # rdd_split = rdd.map(lambda row: row.asDict())   # transforms the RDD, converting each row into a Py dictionary.
        # result = rdd_split.collect()  # pull all data from the distributed RDD into a Python list on the driver
        # logging.info(f"RDD from csv: {type(result), result} ") #<class 'list'> [{'incident_id ': 1, 'incident_type': 'I', 'vin_number': 'VXIO456XLBB630221', 'make': 'Nissan', 'model': 'Altima', 'year': 2003, 'Incident_date': datetime.date(2002, 5, 8), 'Description': 'Initial sales from TechMotors'}, {...}]
        return rdd

    # data_path = utils.get_data_directory()
    #Variables shoudl be nouns
    #Function & Method shoudl be verbs

    #Create mysql connection:
    def mysql_connector(self):
        # MySQL connection properties
        return {
            "mysql_url": "jdbc:mysql://localhost:3308/RBA_EXCHANGE_RATES",
            "table_name": "RBA_EXCHANGE_RATES",
            "user": "root",
            "password": "rootpass"
        }


    def dataframe_to_mysql_writer(self):
        # Write DataFrame to MySQL
        config = self.mysql_connector()
        self.logger.warning(config)
        try:
            self.formatter_sp_df.write \
                .format("jdbc") \
                .option("url", config["mysql_url"]) \
                .option("dbtable", config["table_name"]) \
                .option("user", config["user"]) \
                .option("password", config["password"]) \
                .mode("append") \
                .save()

        except AnalysisException as e:
            print(f"Analysis Exception: {e}")
        except Exception as e:
            print(f"An error occurred while writing to MySQL: {e}")

        # finally:
        #     # Stop the Spark session
        #     spark.stop()


if __name__ == '__main__':
    loade = Loader()
    # loade.dataframe_to_mysql_writer()
    # loade.csv_rdd_loader()
