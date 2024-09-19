from utils import Util
import pandas as pd
from extractors import DailyRatesExtractor
from datetime import datetime


class Cleansers:
    def __init__(self):
        daily_rate_extractor = DailyRatesExtractor()
        self.all_data_extractor = daily_rate_extractor.all_data_extractor()

    def keyless_rates_extractor(self):
        all_data = self.all_data_extractor
        print(all_data)
        keyless_rates_data_dict = {}
        for val in all_data.keys():
            constructed_data = [all_data[val]['code']
                      , all_data[val]['rate']
                   , all_data[val]['date']]
            keyless_rates_data_dict.update({val: constructed_data}) #{'aud': ['AUD', 1.4764267343203, 'Mon, 26 Aug 2024 23:57:21 GMT'], 'cad': ['CAD', 1.3499569571795, 'Mon, 26 Aug 2024 23:57:21 GMT']...
        return keyless_rates_data_dict

    def extract_kwarg_rates(self):
        keys_to_extract = ['code', 'rate', 'date', 'name']
        all_data = self.all_data_extractor
        kwarg_data ={}
        """Extracts specified key-value pairs from a dictionary and creates a new one."""
        for k, v in all_data.items():
            extracted_rates_dict = {key: v[key] for key in keys_to_extract if key in v}
            kwarg_data.update({k: extracted_rates_dict})
        print(kwarg_data)
        return kwarg_data

#DO THIS------->modify above to convert datetime string to only date and send to TRransformer class
    def rates_pandas_df_creator(self):
        rates_data_dict = self.extract_kwarg_rates()
        # print(type(rates_data_dict), "\n", rates_data_dict)
        # Converting to Pandas Dataframe
        df = pd.DataFrame.from_dict(rates_data_dict, orient='index')
        # print(df)
        # Format the datetime object to the acceptable date format
        # ----->This
        # for val in df.keys():
            # df.loc['date',val] = pd.to_datetime(df[val]['date'], format="%a, %d %b %Y %H:%M:%S %Z").strftime("%Y-%m-%d")
        #     df.loc['date', val] = pd.to_datetime(df[val]['date']).date() #----->This
            # df.loc['date', val] = df[val]['date'].date
            # print(type(df.loc['date', val]), "\n", df.loc['date', val])
        # print(df)
        # return df
        return rates_data_dict


    def datetime_to_date_converter(self):
        rates_data_dict = self.keyless_rates_extractor()
        print(type(rates_data_dict))
        for key, val in rates_data_dict.items():
            pass
            # datetime_obj = datetime.strptime(val[2], "%a, %d %b %Y %H:%M:%S %Z")
            # date_only_str = datetime_obj.strftime("%Y-%m-%d")
            # print(date_only_str, "\n", type(date_only_str))


            # print(val[2].strftime('%Y-%m-%d'))
        #convert 'Fri, 23 Aug 2024 23:59:00 GMT' to yyyy-MM-dd.'%Y-%m-%d'
        # pass

if __name__ == '__main__':
    cleansers = Cleansers()
    # rates_blah = cleansers.keyless_rates_extractor()
    # rates_dataframe_creator = cleansers.rates_pandas_df_creator()
    # datetime_converter = cleansers.datetime_to_date_converter()
    kwargs_extracted = cleansers.extract_kwarg_rates()
