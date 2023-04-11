"""
    This file takes data present in S3 bucket
    and creates data frames using spark object

    Functions:
    - get_transactions_raw: loads transaction table from bronze layer
    - get_users: loads users table from bronze layer
    - get_products: loads products table from bronze layer
    - get_clickstream: loads click stream table from bronze layer
"""

from pyspark.sql import SparkSession
from src import S3Layers

spark = SparkSession.builder.getOrCreate()


class TableLoader:

    def __int__(self):
        name_map = self.__create_name_map()

    @staticmethod
    def get_transactions_raw(path: str = S3Layers.BRONZE, data_format: str = "parquet"):
        """
        Returns the raw transactions data stored in bronze layer
        :param data_format: which format the stored data uses. defaults to parquet
        :param path: the path to the bronze layer
        :return: transactions data frame
        """

        transactions_df = (spark
                           .read
                           .option('mergeSchema', 'true')
                           .parquet(f"{path}/transactions/**")
                           )

        return transactions_df

    @staticmethod
    def get_products_raw(path: str = S3Layers.BRONZE, data_format: str = "delta"):
        """
        Returns the raw products table stored in bronze layer
        :param data_format:  which format the stored data uses. defaults to delta
        :param path: the path to bronze layer
        :return: products data frame
        """

        products_df = (spark
                       .read
                       .format(data_format)
                       .load(f"{path}/products")
                       )
        return products_df

    @staticmethod
    def __create_name_map():
        """
        Creates a map for product id and product name
        :return: Spark dataframe with name to product id mapping
        """
        data = [
            ('product0', 'tumbler'),
            ('product1', 'Sweater'),
            ('product2', 'Beanie'),
            ('product3', 'Mug'),
            ('product4', 'Quarter-zip'),
            ('product5', 'Power-bank'),
            ('product6', 'Pen'),
            ('product7', 'Sticker'),
            ('product8', 'Keychain'),
            ('product9', 'Coaster')
        ]

        headers = ['product_id', 'product_name']

        return spark.createDataFrame(data, headers)

    @staticmethod
    def get_name_map(self):
        return self.name_map
