from utils import Util
from loaders import Loader
import logging
from transformers import Transformer

# def print_hi(name):
#     # Use a breakpoint in the code line below to debug your script.
#     print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


class Main:
    def __init__(self):
        util = Util()
        self.spark = util.spark_session_creator()
        if self.spark is None:
            raise RuntimeError("Failed to create a Spark session")
        # Pass the spark session to Loader
        loader = Loader(self.spark)
        self.csv_rdd = loader.csv_rdd_loader()
        logging.info(f"From main, self.csv_rdd: {type(self.csv_rdd), self.csv_rdd} ")  # <class 'list'> [{'incident_id ': 1, 'incident_type': 'I', 'vin_number': 'VXIO456XLBB630221', 'make': 'Nissan', 'model': 'Altima', 'year': 2003, 'Incident_date': datetime.date(2002, 5, 8), 'Description': 'Initial sales from TechMotors'}, {...}]

        # Now pass the loaded csv_rdd to the Transformer class
        transformer = Transformer(self.csv_rdd)
        make_year_list = transformer.missing_data_propogater()  #[('Nissan-2003', 2), ('Mercedes-2015', 1), ('Mercedes-2015', 1), ('Mercedes-2016', 1)]
        # Collect and log the results
        for (make_year, count) in make_year_list:
            logging.info(f"{make_year},{count}")

    # Transform data
    def data_transformer(self):
        pass
    # transformed_data = transform_data(data)

    # Load data to MySQL
    def data_loader(self):
        pass
    # load_data(transformed_data, spark)

    # Stop the Spark session
    # spark.stop()



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main_instance = Main()
    # logging.info(f"Spark Session started successfully: {main_instance.extract_data()} ")

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
