import src.utils.load_tables as load_tables
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, explode, month, year, lit, when, lag, max, sum, round
from src.utils.S3Layers import S3Layers

spark = SparkSession.builder.getOrCreate()


class MoMTransformer:

    def __init__(self):
        self.transactions_filtered = None
        self.transactions = None
        self.transactions_df = load_tables.get_transactions(path=S3Layers.SILVER.value, data_format='delta')
        self.products_df = load_tables.get_products(path=S3Layers.BRONZE.value)
        self.name_mapping_df = load_tables.name_map

    def __get_product_id(self, item: str) -> str:
        product_id = (self.name_mapping_df
                      .select('product_id')
                      .filter(col('product_name') == item)
                      .collect())[0]['product_id']

        return product_id

    def __get_item_cost(self, product_id: str, ):
        item_cost = (self.products_df
                     .select('price')
                     .filter(col('product_id') == product_id)
                     .collect()
                     )[0]['price']
        return item_cost

    def __get_purchases(self):
        return self.transactions_filtered.filter(col('transaction_type') == 'purchase')

    def __get_returns(self):
        return self.transactions_filtered.filter(col('transaction_type') == 'return')

    def transform(self, product: str):
        product_id = self.__get_product_id(product)
        item_cost = self.__get_item_cost(product_id)

        self.transactions = self.transactions_df.filter(col('utc_date').between('2020-03-01', '2023-03-31'))

        transactions_exploded = (
            self.transactions
            .select(
                "order_id",
                'email',
                'transaction_type',
                explode('items').alias('item'),
                'utc_date'
            )
        )

        self.transactions_filtered = transactions_exploded.filter(col('item') == product_id)

        self.transactions_filtered = (
            self.transactions_filtered
            .select(
                '*',
                year(col('utc_date')).alias('transaction_year'),
                month(col('utc_date')).alias('transaction_month')
            )
        )

        purchases = self.__get_purchases()
        returns = self.__get_returns()

        returns_with_purchase_date = (
            returns.alias('r')
            .join(purchases.alias('p'), (col('r.utc_date') >= col('p.utc_date')) & (
                    col('r.email') == col('p.email')), "left")
            .select(
                col('r.email').alias('email'),
                col('r.order_id'),
                col('p.utc_date').alias('purchase_utc_date'),
                col('r.utc_date').alias('return_utc_date')
            )
            .orderBy(col('r.email'))
        )

        returns_with_purchase_date = (
            returns_with_purchase_date
            .select(
                'email',
                'purchase_utc_date',
                'return_utc_date'
            )
            .groupBy('email', 'return_utc_date')
            .agg(max('purchase_utc_date').alias('purchase_utc_date'))
        )

        returns_with_purchase_date = (
            returns_with_purchase_date
            .select(
                'email',
                'purchase_utc_date',
                'return_utc_date'
            )
        )

        purchased_without_return = (
            purchases
            .select(
                "email",
                col("utc_date").alias("purchase_utc_date"),
                lit(None).alias("return_utc_date")
            )
            .join(returns.select("email"), ["email"], "left_anti"))

        product_sales_df = purchased_without_return.union(returns_with_purchase_date)

        product_sales_df = (
            product_sales_df
            .select(
                when(
                    col('return_utc_date').isNotNull() & col('purchase_utc_date').isNull(),
                    col('return_utc_date')
                )
                .otherwise(col('purchase_utc_date')).alias('transaction_date'),

                when(
                    col('return_utc_date').isNotNull() & col('purchase_utc_date').isNull(),
                    -1
                )
                .when(
                    col('return_utc_date').isNull() & col('purchase_utc_date').isNotNull(),
                    1
                )
                .otherwise(
                    0
                ).alias('item_count')

            )
            .orderBy('transaction_date')
        )

        sales_report_temp = (
            product_sales_df
            .select(
                year(col('transaction_date')).alias('transaction_year'),
                month(col('transaction_date')).alias('transaction_month'),
                'item_count'
            )
            .groupBy('transaction_year', 'transaction_month')
            .agg(sum(col('item_count')).alias('total_items_sold'))
            .orderBy('transaction_year', 'transaction_month')
        )

        sales_report = (
            sales_report_temp
            .select(
                '*',
                round(col('total_items_sold') * item_cost, 2).alias('total_sale')
            )
            .orderBy('transaction_year', 'transaction_month')
        )

        sales_report_with_prev_month = (
            sales_report
            .select(
                '*',
                lag("total_sale", offset=1)
                .over(Window.orderBy('transaction_year', 'transaction_month'))
                .alias('previous_month_sale'),
            )
            .orderBy('transaction_year', 'transaction_month')
        )

        sales_report_final = (
            sales_report_with_prev_month
            .select(
                'transaction_year',
                'transaction_month',
                'total_items_sold',
                'total_sale',
                (round(((col('total_sale') - col('previous_month_sale')) / (
                        (col('total_sale') + col('previous_month_sale')) / 2)), 2) * 100).cast('string').alias(
                    'percentage_difference')
            )
        )

        return sales_report_final
