from S3Layers import S3Layers
import load_tables
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max

spark = SparkSession.builder.getOrCreate()


def get_latest_transaction_date():
    transactions_df = load_tables.get_transactions(S3Layers.BRONZE.value)

    return transactions_df.agg(max(col('utc_date')).alias('last_date')).collect()[0]['last_date']