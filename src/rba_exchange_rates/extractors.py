from utils import Util
import requests
import json
# import pyarrow.parquet as pq

# Variables shoudl be nouns
# Function & Method shoudl be verbs
# data_path = utils.get_data_directory()


class DailyRatesExtractor:
    def __init__(self):
        utils = Util()
        self.source_data_url = utils.source_url_getter()

    def all_data_extractor(self):
        print(self.source_data_url)
        response = requests.get(self.source_data_url)
        print(response.status_code)
        pretty_json = json.loads(response.text)
        # Task 1: Trigger another thread and store(Multi-threading)
        return pretty_json


if __name__ == '__main__':
    extractor = DailyRatesExtractor()
    # data = extractor.all_data_extractor()
    # print(data)

