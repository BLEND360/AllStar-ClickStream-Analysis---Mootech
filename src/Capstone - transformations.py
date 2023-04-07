# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Data Frames

# COMMAND ----------

# MAGIC %md
# MAGIC #### Importing libraries

# COMMAND ----------

from pyspark.sql.functions import col, min, max, round, date_format, count, when
from pyspark.sql.functions import to_date, explode, month, year, dayofmonth, sum

# COMMAND ----------

# MAGIC %md
# MAGIC #### Products dataframe

# COMMAND ----------

products_path = "s3://allstar-training-mootech/raw_data/products"

products_df = spark.read.format('delta').load(products_path)

# COMMAND ----------

products_df.orderBy('product_id').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transactions dataframe

# COMMAND ----------

path_to_transactions = 's3://allstar-training-mootech/raw_data/transactions/**'

transactions_df = spark.read.option('mergeSchema', 'true').parquet(path_to_transactions)  


# COMMAND ----------

transactions_df.orderBy('order_id').limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Clickstream dataframe

# COMMAND ----------

clickstream_path = "s3://allstar-training-mootech/tables/clickstream"

clickstream_df = spark.read.parquet(clickstream_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Product ID to Product Name mapping

# COMMAND ----------

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

name_mapping_df = spark.createDataFrame(data, headers)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformations

# COMMAND ----------

'''
Select the product id for given product name from name mapping and assign
returned id to the variable product_id.

This variable will be used to filter data frames according to required product
'''

ITEM_TO_BE_QUERIED = 'tumbler'

product_id = (name_mapping_df
              .select('product_id')
              .filter(col('product_name') == ITEM_TO_BE_QUERIED)
              .collect())[0]['product_id']

# COMMAND ----------

# MAGIC %md
# MAGIC #### Modified Transactions DF

# COMMAND ----------

'''
Selecting relevant columns from transactions dataframe.

The month and year of transaction is extracted from the utc_date Column

"items" column consists of an array of products bought in the order which was split into
individual rows

'''

transactions_exploded_df = (transactions_df
        .select(
            'order_id',
            'transaction_type',
            dayofmonth(col('utc_date')).alias('transaction_day'),
            month(col('utc_date')).alias('transaction_month'),
            year(col('utc_date')).alias('transaction_year'),
            explode('items').alias('item_id')
        )
       )



# COMMAND ----------

# MAGIC %md
# MAGIC #### Filtered Transactions DF

# COMMAND ----------

'''
A filter is  applied so that we only get the rows where the product id is the one 
required for our analysis.

Finally, group by the year and month and then count the number of transactions
recorded with given product id.
'''

filtered_transaction_df = (transactions_exploded_df
        .select(
            'transaction_year',
            'transaction_month',
            'item_id'
        )
        .filter((col('item_id') == product_id))
        .groupBy('transaction_year', 'transaction_month')
        .agg(count('item_id').alias('total_transactions'))
        .orderBy('transaction_year', 'transaction_month')
       )

# COMMAND ----------

filtered_transaction_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Items returned DF

# COMMAND ----------

'''

Creating a dataframe that calculates the total number of 
returned items with given product id

'''

returned_items_df = (transactions_exploded_df
                     .select(
                         'transaction_year',
                         'transaction_month',
                         'transaction_type',
                         'item_id'
                     )
                     .filter((col('item_id') == product_id) & (col('transaction_type') == 'return'))
                     .groupBy('transaction_year', 'transaction_month')
                     .agg(count('item_id').alias('total_returns'))
                     .orderBy('transaction_year', 'transaction_month')
                    )

# COMMAND ----------

returned_items_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Items sold

# COMMAND ----------

'''

Creating a dataframe with the year, month and total sales

Joining filtered transactions and returned items to calculate
total sales
'''

items_sold_df = (filtered_transaction_df
                 .join(returned_items_df, ['transaction_year', 'transaction_month'], "leftouter")
                 .select(
                     'transaction_year',
                     'transaction_month',
                     when(
                         col('total_returns') > 0, col('total_transactions') - col('total_returns')
                     )
                     .otherwise(col('total_transactions') - 0)
                     .alias('total_sales')
                 )
                 .orderBy('transaction_year', 'transaction_month')
                )

# COMMAND ----------

items_sold_df.display()

# COMMAND ----------

'''
Getting the individual cost for item in question
'''

item_cost = (products_df
             .select('price')
             .filter(col('product_id') == product_id)
             .collect()
            )[0]['price']

print(item_cost)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Monthly Sales DF

# COMMAND ----------

'''
Final dataframe for monthly sales of required item over last 3 years.

The transaction month column is changed from being a number to the
corresponding month name.

Monthly sale is calculated as the item sale count for that month multiplied
by the item cost we get from the products data frame, rounded to 2 decimal places.
'''



monthly_sales_df = (items_sold_df
                    .select(
                        'transaction_year',
                        date_format(to_date(col('transaction_month'), "M"), "MMM").alias('transaction_month'),
                        'total_sales',
                        round(col('total_sales') * item_cost, 2).alias('Monthly_Sale_usd')
                    )
                    
                   )

# COMMAND ----------

monthly_sales_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Store in S3

# COMMAND ----------

destination_path = "s3://allstar-training-mootech/results"

# COMMAND ----------

'''
Final data frame - stored as a parquet file in specified destination path
write mode - set to overwrite.
'''

monthly_sales_df.write.mode('overwrite').parquet(f"{destination_path}/monthly_sale_{ITEM_TO_BE_QUERIED}")
