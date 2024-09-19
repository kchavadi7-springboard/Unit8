from transformers import Transformer
import logging
from pyspark.sql.functions import expr



class Analyzer:
    def __init__(self):
        transformer = Transformer()
        self.logger = logging.getLogger(__name__)
        self.formatter_sp_df = transformer.formatted_sp_dataframe()

    def top_ten_currency_calculator(self):
        df = self.formatter_sp_df.orderBy(self.formatter_sp_df["rate"].desc()).limit(10).show()
        # persist this as next function is on this extracted dataset
        #is it needed, does it compute rdd again just to get 1 date
        rate_date = self.formatter_sp_df.select("date").limit(1).show()
        top_ten_rates_df = self.formatter_sp_df.select("rate", "name").orderBy(self.formatter_sp_df["rate"].desc()).limit(10).show()
        # self.logger.warning(f"Top 10 currency rates for {self.formatter_sp_df.select("date").limit(1).show()} are: {top_ten_rates_df}")


# Look at transformers class top_ten_currency_calculator(self): method for is any missing info
# converted_spDF = spDF.rdd.map(lambda x: {x['code']: float(x['rate'])}).collect()
# USE THIS AND BOTTOM EXMAPLE WHEN AGGREGATING AND GETTING ANALYSIS REULT
        #THIS
        # nLength = spDF.count()
        # for val in converted_spDF:
        #     # print(val)
        #     print(converted_spDF.rdd.groupByKey().map(lambda x: (x[0], list(x[1])[:nLength])).collect())
#https://stackoverflow.com/questions/31882221/spark-select-top-values-in-rdd
            #& THSI
        #LOOK INTO THIS AND EXTRACT ONLY LAST 2 ELEMENTS FOR EACH VALUE OF A DICT ELEMENT
        # my_dict = {
        #     "key1": [10, 20, 30],
        #     "key2": [40, 50, 60],
        #     "key3": [70, 80, 90]
        # }
        # n = 1  # Index of the element you want to access (e.g., 2nd element)
        # for key, value in my_dict.items():
        #     if n < len(value):
        #         print(f"Element at index {n} for key '{key}': {value[n]}")
        #     else:
        #         print(f"Key '{key}' does not have an element at index {n}")

        # print(e_spDF.toDebugString())

        # print(df.toDebugString().decode("utf-8"))
        #Calculating the top 10 currencies from Data.
        # res = spDF.rdd.top(10)

        # print(self.rdd.top(2, key=lambda x: x[2]))
        # return self.rdd.top(2, key=lambda x: x[2])


if __name__ == '__main__':
    analyzer = Analyzer()
    analyzer.top_ten_currency_calculator()