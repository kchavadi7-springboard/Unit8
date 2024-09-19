import importlib.metadata
from typing import Any
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
import pyspark.sql.dataframe
from pyspark.sql import SparkSession
from utils import Util
from dotenv import load_dotenv
from cleansers import Cleansers
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import to_json, struct
from pyspark.sql.functions import to_date, col
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
from decimal import Decimal
from pyspark.sql.functions import col, explode, struct
from datetime import datetime
import logging



#  top currencies or close rate in the last week
# The Biggest difference in fx rate within a day in USD
#log.info()
class Transformer:
    def __init__(self, csv_rdd):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.csv_rdd = csv_rdd
        logging.info(f"From T, self.csv_rdd: {type(self.csv_rdd), self.csv_rdd} ")  # <class 'list'> [{'incident_id ': 1, 'incident_type': 'I', 'vin_number': 'VXIO456XLBB630221', 'make': 'Nissan', 'model': 'Altima', 'year': 2003, 'Incident_date': datetime.date(2002, 5, 8), 'Description': 'Initial sales from TechMotors'}, {...}]

        # cleansers = Cleansers()
        # self.pdDF = cleansers.rates_pandas_df_creator()
        # print("From Transformers: ", type(self.pdDF), self.pdDF ) # <class 'pandas.core.frame.DataFrame'>

    @staticmethod
    def extract_vin_key_value(record) -> tuple[Any, tuple[Any, Any, Any, Any, Any, Any, Any]]:
        logging.info(f"Records to k-v pair: {type(record), record} ")  # <class 'list'> [{'incident_id ': 1, 'incident_type': 'I', 'vin_number': 'VXIO456XLBB630221', 'make': 'Nissan', 'model': 'Altima', 'year': 2003, 'Incident_date': datetime.date(2002, 5, 8), 'Description': 'Initial sales from TechMotors'}, {...}]
        # Create key-value pair (vin_number, (make, year, incident_type, Incident_date))
        return (record.vin_number, (record.incident_id, record.incident_type, record.make, record.model, record.year, record.incident_date, record.description))

    @staticmethod
    def make_year_populater(kv):
        logging.info(f"make_year_populater: {kv}")

    def missing_data_propogater(self):
        logging.info(f"Hello 1")
        vin_kv_rdd = self.csv_rdd.map(lambda record: Transformer.extract_vin_key_value(record))
        # Group by vin_number: VIN: INU45KIOOPA343980, Records: [(2, 'I', 'Mercedes', 'C300', 2015, datetime.date(2014, 1, 1), 'Sold from EuroMotors'), (16, 'A', None, None, None, datetime.date(2020, 5, 1), 'Side collision')]
        grouped_by_vin = vin_kv_rdd.groupByKey()
        grouped_by_data = grouped_by_vin.collect()  # To trigger action
        # List to store make-year and count of 'A' occurrences
        make_year_rdd = []
        for vin, records in grouped_by_data:
            list_records = list(records)
            make = None
            year = None

            for val in list_records:
                # logging.info(f"list_rec: {val, val[0]}")
                if val[2] is not None and val[4] is not None:
                    make = val[2]
                    year = val[4]
                    break
                # Found Make & Year
                logging.info(f"VIN: {vin}, Found Make: {make}, Year: {year}")

            if make and year:
                # Propagate make and year to records that are missing them
                filled_records = [(make or rec[2], year or rec[4], rec[1]) for rec in list_records]

                logging.info(f"VIN: {vin}, Filled Records: {filled_records}")
                # Count occurrences of A
                count = 0
                count_map = {}
                for rec in filled_records:
                    if rec[2] == 'A':
                        key = f"{rec[0]}-{rec[1]}"
                        if key in count_map:
                            count_map[key] += 1
                        else:
                            count_map[key] = 1
                logging.info(f"count_map: {count_map}")

                # Extend the list with make-year and count occurrences
                make_year_rdd.extend(count_map.items())
                logging.info(f"make_year_rdd: {make_year_rdd}")
        return make_year_rdd



        # return grouped_by_vin




    # def spark_session_creator(self):
    #     """Create a Spark Session"""
    #     _ = load_dotenv()
    #     # Initialize Spark session with the MySQL JDBC driver
    #     try:
    #         spark = SparkSession.builder \
    #             .appName("CurrencyRate") \
    #             .config("spark.jars", "/Users/kiranchavadi/Documents/KarthikNewAssignments/DE2024/May_2024/rba_exchange_rates/libs/mysql-connector-j-9.0.0/mysql-connector-j-9.0.0.jar") \
    #             .config("spark.driver.extraClassPath", "/Users/kiranchavadi/Documents/KarthikNewAssignments/DE2024/May_2024/rba_exchange_rates/libs/mysql-connector-j-9.0.0/mysql-connector-j-9.0.0.jar") \
    #             .getOrCreate()
    #             # Log to confirm the classpath includes the MySQL JDBC driver
    #         print(spark.sparkContext.getConf().get("spark.jars"))
    #     except Exception as e:
    #         print(f"An error occurred while creating the Spark session: {e}")
        # finally:
        #     if 'spark' in locals():
        #         spark.stop()
        #         print("Spark session stopped.")

        # Check the list of registered JDBC drivers
        # try:
        #     driver_class = "com.mysql.cj.jdbc.Driver"
        #     spark._jvm.java.lang.Class.forName(driver_class)
        #     print(f"Driver {driver_class} is available.")
        # except Exception as e:
        #     print(f"Failed to load driver {driver_class}: {e}")
        # return spark

    # Function to parse the date string
    @staticmethod
    def parse_date(date_str: Any) -> datetime:
        # Parse the date string to a datetime object
        date_obj = datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S %Z')
        just_date = date_obj.date()
        return just_date

    def formatted_sp_dataframe(self):
        # Creating Spark Session
        spark = self.spark_session_creator()
        # print("Spark session started successfully: ", spark)
        self.logger.warning("Spark session started successfully: ")


        # Define the schema for the nested dictionary
        currency_schema = StructType([
            StructField("currency", StringType(), True),
            StructField("code", StringType(), True),
            StructField("rate", DoubleType(), True),
            StructField("date", DateType(), True),
            StructField("name", StringType(), True)
            ])
        # Convert dictionary to list of tuples: [('aud', {'code': 'AUD', 'rate': 1.4691769710729, 'date': 'Fri, 30 Aug 2024 11:57:16 GMT', 'name': 'Australian Dollar'}),
        data = list(self.pdDF.items())
        # self.logger.warning(data)

        # Convert to a list of dictionaries: [{'currency': 'aud', 'code': 'AUD', 'rate': 1.4691769710729, 'date': 'Fri, 30 Aug 2024 11:57:16 GMT', 'name': 'Australian Dollar'},
        # Prep data to rdd acceptable format: Convert Pandas DataFrame to a list of rows (each row is a dict or tuple). Assuming pdDF is already a Pandas DataFrame
        rows = [
            {
                'currency': currency_code,
                'code': details['code'],
                'rate': details['rate'],
                'date': details['date'],
                'name': details['name']
            }
            for currency_code, details in data
        ]
        # Update the rows with the parsed date and all rate column to float (for some ints to flat)
        # print("rows BEFORE:", rows)
        for row in rows:
            row['rate'] = float(row['rate'])
            row['date'] = self.parse_date(row['date'])
        # print("rows AFTER:",rows)
        self.logger.warning("rows AFTER:")

        # Convert dictionary to RDD
        rdd = spark.sparkContext.parallelize(rows)
        self.logger.warning(rdd.collect()) #rdd.collect()[0]['currency']

        # Convert the list of dictionaries to a Spark DataFrame
        df = spark.createDataFrame(rows, schema=currency_schema)

        # Show the DataFrame schema and contents (optional)
        df.printSchema()
        df.show()
        return df

        # rdd_dict=rdd.collectAsMap()
        # rdd_list= list(rdd_dict.values())
        # print(rdd_list)  # [{'code': 'AUD', 'rate': 1.468330276601, 'date': 'Thu, 29 Aug 2024 18:55:06 GMT', 'name': 'Australian Dollar'},

        # Convert the data to rows with schema
        # data_rows = [{ value['code'], value['rate'], value['date'], value['name']} for value in rdd]
        # print(data_rows)
        # [{1.468330276601, 'Thu, 29 Aug 2024 18:55:06 GMT', 'AUD', 'Australian Dollar'}, {1.3460360683063, 'Thu, 29 Aug 2024 18:55:06 GMT', 'Canadian Dollar', 'CAD'}, {'CNY', 'Chinese Yuan', 'Thu, 29 Aug 2024 18:55:06 GMT', 7.1000552323994}, {'EUR', 0.90077092176582, 'Thu, 29 Aug 2024 18:55:06 GMT', 'Euro'}, {0.75810512913436, 'Thu, 29 Aug 2024 18:55:06 GMT', 'U.K. Pound Sterling', 'GBP'}, {'Hong Kong Dollar', 'Thu, 29 Aug 2024 18:55:06 GMT', 7.7958743866903, 'HKD'}, {'IDR', 'Indonesian Rupiah', 15400.076133216, 'Thu, 29 Aug 2024 18:55:06 GMT'}, {'INR', 83.889391524085, 'Thu, 29 Aug 2024 18:55:06 GMT', 'Indian Rupee'}, {144.63958736939, 'JPY', 'Thu, 29 Aug 2024 18:55:06 GMT', 'Japanese Yen'}, {'KRW', 1331.2109132064, 'Thu, 29 Aug 2024 18:55:06 GMT', 'South Korean Won'}, {'Thu, 29 Aug 2024 18:55:06 GMT', 4.3173224114338, 'Malaysian Ringgit', 'MYR'}, {1.5919794523624, 'NZD', 'Thu, 29 Aug 2024 18:55:06 GMT', 'New Zealand Dollar'}, {56.213838472538, 'PHP', 'Thu, 29 Aug 2024 18:55:06 GMT', 'Philippine Peso'}, {1.3011348193481, 'SGD', 'Thu, 29 Aug 2024 18:55:06 GMT', 'Singapore Dollar'}, {33.960867008294, 'Thu, 29 Aug 2024 18:55:06 GMT', 'THB', 'Thai Baht'}, {'New Taiwan Dollar ', 'Thu, 29 Aug 2024 18:55:06 GMT', 31.893638901131, 'TWD'}, {24866.191763248, 'Thu, 29 Aug 2024 18:55:06 GMT', 'VND', 'Vietnamese Dong'}, {'Bulgarian Lev', 1.7639171608049, 'Thu, 29 Aug 2024 18:55:06 GMT', 'BGN'}, {'Brazilian Real', 'BRL', 'Thu, 29 Aug 2024 18:55:06 GMT', 5.5810784400366}, {0.84453874067632, 'Thu, 29 Aug 2024 18:55:06 GMT', 'Swiss Franc', 'CHF'}, {911.81325159161, 'Thu, 29 Aug 2024 18:55:06 GMT', 'Chilean Peso', 'CLP'}, {'Czech Koruna', 'CZK', 'Thu, 29 Aug 2024 18:55:06 GMT', 22.586463191654}, {'Danish Krone', 'Thu, 29 Aug 2024 18:55:06 GMT', 'DKK', 6.7269093674768}, {'Hungarian Forint', 'HUF', 'Thu, 29 Aug 2024 18:55:06 GMT', 354.20788794415}, {'ILS', 3.6678321667656, 'Thu, 29 Aug 2024 18:55:06 GMT', 'Israeli New Sheqel'}, {137.90187680107, 'ISK', 'Icelandic Krona', 'Thu, 29 Aug 2024 18:55:06 GMT'}, {19.706198813673, 'MXN', 'Thu, 29 Aug 2024 18:55:06 GMT', 'Mexican Peso'}, {10.498750873358, 'Thu, 29 Aug 2024 18:55:06 GMT', 'NOK', 'Norwegian Krone'}, {'PLN', 3.8687908457742, 'Thu, 29 Aug 2024 18:55:06 GMT', 'Polish Zloty'}, {'Romanian New Leu', 'Thu, 29 Aug 2024 18:55:06 GMT', 4.4890749972508, 'RON'}, {10.230880989809, 'SEK', 'Thu, 29 Aug 2024 18:55:06 GMT', 'Swedish Krona'}, {'TRY', 34.075783211405, 'Thu, 29 Aug 2024 18:55:06 GMT', 'Turkish Lira'}, {'UAH', 41.226012793179, 'Thu, 29 Aug 2024 18:55:06 GMT', 'Ukrainian Hryvnia'}, {17.708787259023, 'ZAR', 'Thu, 29 Aug 2024 18:55:06 GMT', 'South African Rand'}, {'EGP', 48.67197875166, 'Thu, 29 Aug 2024 18:55:06 GMT', 'Egyptian Pound'}, {0.70896605087533, 'JOD', 'Thu, 29 Aug 2024 18:55:06 GMT', 'Jordanian Dinar'}, {91624.999999999, 'LBP', 'Thu, 29 Aug 2024 18:55:06 GMT', 'Lebanese Pound'}, {'BDT', 'Bangladeshi taka', 'Thu, 29 Aug 2024 18:55:06 GMT', 119.43913538111}, {'Belarussian Ruble', 3.2725600822917, 'Thu, 29 Aug 2024 18:55:06 GMT', 'BYN'}, {'PKR', 'Thu, 29 Aug 2024 18:55:06 GMT', 278.99813978209, 'Pakistani Rupee'}, {'XAF', 'Central African CFA Franc', 'Thu, 29 Aug 2024 18:55:06 GMT', 591.58123873443}, {'West African CFA Franc', 'Thu, 29 Aug 2024 18:55:06 GMT', 'XOF', 591.58123873443}, {'Thu, 29 Aug 2024 18:55:06 GMT', 'Nigerian Naira', 1595.8045, 'NGN'}, {'Saudi Riyal', 3.7526999351193, 'Thu, 29 Aug 2024 18:55:06 GMT', 'SAR'}, {'DOP', 'Dominican Peso', 59.544130754847, 'Thu, 29 Aug 2024 18:55:06 GMT'}, {'Venezuelan Bolivar', 'Thu, 29 Aug 2024 18:55:06 GMT', 36.548747882059, 'VES'}, {'Peruvian Nuevo Sol', 3.7388221583477, 'Thu, 29 Aug 2024 18:55:06 GMT', 'PEN'}, {'RUB', 'Russian Rouble', 91.638960472877, 'Thu, 29 Aug 2024 18:55:06 GMT'}, {'U.A.E Dirham', 3.6726914510878, 'Thu, 29 Aug 2024 18:55:06 GMT', 'AED'}, {'Argentine Peso', 'ARS', 'Thu, 29 Aug 2024 18:55:06 GMT', 948.82434301522}, {'Thu, 29 Aug 2024 18:55:06 GMT', 'BOB', 'Bolivian Boliviano', 6.8599999999998}, {'COP', 4083.3333333333, 'Thu, 29 Aug 2024 18:55:06 GMT', 'Colombian Peso'}, {'Costa Rican Colón', 'CRC', 'Thu, 29 Aug 2024 18:55:06 GMT', 518.91787065052}, {'Algerian Dinar', 'Thu, 29 Aug 2024 18:55:06 GMT', 133.87978142076, 'DZD'}, {'Haitian gourde', 131.46799540054, 'Thu, 29 Aug 2024 18:55:06 GMT', 'HTG'}, {'PAB', 1, 'Thu, 29 Aug 2024 18:55:06 GMT', 'Panamanian Balboa'}, {'Thu, 29 Aug 2024 18:55:06 GMT', 'Paraguayan Guaraní', 7622.222222222, 'PYG'}, {'Tunisian Dinar', 'TND', 3.0435997888114, 'Thu, 29 Aug 2024 18:55:06 GMT'}, {40.246406570841, 'Thu, 29 Aug 2024 18:55:06 GMT', 'UYU', 'Uruguayan Peso'}, {17.490302560124, 'Thu, 29 Aug 2024 18:55:06 GMT', 'MDL', 'Moldova Lei'}, {'RSD', 106.09411764706, 'Thu, 29 Aug 2024 18:55:06 GMT', 'Serbian Dinar'}]

        # Convert RDD to DataFrame with schema
        # df = spark.createDataFrame(data_rows, currency_schema)
        #
        # # Show the DataFrame
        # df.show()

        # # Define the schema for the main dictionary (mapping from currency codes to their details)
        # schema = MapType(StringType(), currency_schema)
        #
        # # Convert the data dictionary to a Spark DataFrame with the schema
        # df = spark.createDataFrame([(self.pdDF,)], schema=StructType([StructField("currency_data", schema, True)]))

        # # Explode the dictionary into multiple rows
        # df_exploded = df.selectExpr("explode(currency_data) as (currency, details)")
        #
        # # Select the 'code' and 'rate' fields
        # df_rates = df_exploded.select(col("details.code").alias("code"), col("details.rate").alias("rate"))
        #
        # # Sort by rate in descending order
        # df_sorted = df_rates.orderBy(col("rate").desc())
        #
        # # Get the top 10 rates
        # top_10_df = df_sorted.limit(10)
        #
        # # Convert the DataFrame to a dictionary
        # top_10_dict = {row["code"]: row["rate"] for row in top_10_df.collect()}
        #
        # # Show the results
        # print(top_10_dict)

        # # schema = StructType({
        # #     StructField('country',StringType(), True),
        # #     StructField('rate_values', MapType(StringType(),DecimalType(),DateType()), True)})
        #     #             StructType({
        #     #     StructField('code',StringType(),True),
        #     #     StructField('rate',DecimalType(),True),
        #     #     StructField('date',DateType(),True)
        #     # }))})
        # # }){'aud': {'code': 'AUD', 'rate': 1.4690803870977, 'date': 'Thu, 29 Aug 2024 09:57:22 GMT', 'name': 'Australian Dollar'}, 'cad': {
        # #Converting Pandas DF to Spark DF  DataFrame[aud: string, cad: string,...]
        # spDF = spark_session.createDataFrame(self.pdDF)
        # print("After converting to Spark DF:", type(spDF), spDF.collect())  #<class 'pyspark.sql.dataframe.DataFrame'>
        # # spDF2 = spDF.select(col('date')).collect()
        # spDF_date = list(dict.fromkeys(spDF.rdd.map(lambda x: x.date).collect()))
        # print("Distinct Dates: ",spDF_date)
        # # Convert string to date, get desc lim 1/distinct
        # # converted_spDF =spDF.rdd.map(lambda x:{x['code']: float(x['rate'])}).collect()
        # converted_spDF = spDF.rdd.map(lambda x: [x['code'], float(x['rate']), date(x['date']), x['name']]).collect()
        # print(converted_spDF)

        # window_rates = Window.partitionBy('code').orderBy(col('rate').desc()) #<pyspark.sql.window.WindowSpec object at 0x1069445b0>
        # print(window_rates)
        # spDF.withColumn("row",row_number().over(window_rates)) \
        #     .filter(col("row") <= 11) \
        #     .show()

        # print(spDF.select(col("rate").cast(DecimalType).alias("rate")))
        # spDF_updated_schema = spDF.select(
        #     col('code').cast('string'),
        #     col('rate').cast('double'),
        #     to_date(col('Date'),"%a, %d %b %Y %H:%M:%S %Z").cast('date')
        # )
        # print(spDF_updated_schema)
        # spDF_updated = pyspark.sql.dataframe.DataFrame(spDF)
        # Convert columns to a dictionary column
        # spark_df = spDF.select(to_json(struct(*spDF.columns)).alias('data'))
        # # Show the resulting Spark DataFrame
        # spark_df.show()




if __name__ == '__main__':
    transformer = Transformer()
    # spark = transformer.spark_session_creator()
    # transformer.formatted_sp_dataframe()
    transformer.missing_data_propogater()

