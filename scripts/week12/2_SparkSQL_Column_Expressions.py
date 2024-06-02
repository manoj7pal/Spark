from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import col, column, concat, lit, expr
import re
from datetime import datetime

# Spark Configuration
sparkConf = SparkConf()
sparkConf.set('spark.app.name', "Orders Processing")
sparkConf.set('spark.master', 'local[*]')

# Spark Session
spark = SparkSession.builder \
    .config(conf = sparkConf) \
    .getOrCreate()

# Set LogLevel
spark.sparkContext.setLogLevel("WARN")

# Input - sample, with header, and inferSchema argument
orders_df = spark.read.csv(path="dataset/orders.csv", header=True, inferSchema=True)
print(orders_df.printSchema())

# 2 ways - Dataframe API(2ways - .withColumn() and .selectExpr()) and SQL Queries
# --------------------------------------------------
# 1. Column Expressions - Datafram API --> .withColumn(expr()) OR df.selectExpr()
# orders_df = orders_df.withColumn('new_order_status', concat(col('order_status'), lit('_STATUS')) ) \
                    # .withColumn('new_new_order_status', expr('CASE WHEN order_status = "CLOSED" THEN "CANCELLED" ELSE "NOT CANCELLED" END'))
# OR

orders_df = orders_df.selectExpr("order_id", 
                                 "order_date", 
                                 "concat(order_status, '_STATUS') as new_order_status",
                                 "CASE WHEN order_status='CLOSED' THEN 'CANCELLED' ELSE 'NOT CANCELLED' END as new_new_order_status ")

print(orders_df.show())
print(orders_df.printSchema())

# 2. Column Expressions - SQL API
# orders_df.createTempView('orders')

# result_df = spark.sql(
#     'SELECT order_id, order_date, order_customer_id, order_status,  \
#         CONCAT(order_status,"_STATUS") as new_order_status, \
#         CASE WHEN order_status="CLOSED" THEN "CANCELLED" ELSE "NOT CANCELLED" END as new_new_order_status   \
#         FROM orders'
# )

# print(result_df.show())
# print(result_df.printSchema())

# ---------------------------------------------
'''
CONCLUSION:

So based on our unbderstabnding there are 3 ways in total to define Column Expressions:
    1. DF API --> df.withColumn and expr()
    2. DF API --> df.selectExpr()
    3. SQL API --> using plain SQL Queries

Out of all the 3 ways:
2. DF API --> df.slectExpr(): is the most cleanest and readable form of the column expressions
3. SQL API --> is SQL-like so very easy, no learning curve

We can use any of the 2 approaches above.

'''
spark.stop()

