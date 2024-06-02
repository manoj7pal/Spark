from pyspark.sql import SparkSession
from pyspark import SparkConf
import sys

# Spark Configuration
sparkConf = SparkConf()
sparkConf.set('spark.app.name', "My First Application")
sparkConf.set('spark.master', 'local[*]')

# Spark Session
spark = SparkSession.builder \
    .config(conf = sparkConf) \
    .enableHiveSupport()    \
    .getOrCreate()

# Set LogLevel
spark.sparkContext.setLogLevel("WARN")

# Input - sample, with header, and inferSchema argument
orders_df = spark.read.csv(path="dataset/orders.csv", header=True, inferSchema=True)

# Create Temp View
orders_df.createOrReplaceTempView('orders')

# result_df = spark.sql(
#     'SELECT order_status, count(*) as total_count  \
#     FROM orders \
#     GROUP BY order_status \
#     ORDER BY 2 DESC \
#         ')

# Basic Operations using SparkSQl API
result_df = spark.sql(
    'SELECT order_customer_id, count(*) as total_orders \
    FROM orders \
    WHERE order_status = "CLOSED"   \
    GROUP BY order_customer_id  \
    ORDER BY 2 DESC'
    )

# Save the result in the form of Table: Spark Warehouse, Metadata: Derby(in-memory --> temporary)
# result_df.write.format("csv")   \
#     .mode('overwrite')  \
#     .saveAsTable('orders1')

# ------------------------
# USING HIVE SUPOPORT
# ------------------------
# Since, spark metadata is temporary and it gets flushed after the process completes, we can use Hive metastore to persist it for our reference.
    # - To do this, we have to enable Hive Support while creating SparkSession, SparkSession.builder.enableHiveSupport()   

# Create database:
spark.sql("CREATE DATABASE IF NOT EXISTS retail")

# Now store it in this database
result_df.write \
    .format('csv')  \
    .mode('overwrite')  \
    .bucketBy(4, 'order_customer_id')   \
    .sortBy('order_customer_id')    \
    .saveAsTable('retail.orders')

# List tables
print(spark.catalog.listTables('retail'))

spark.stop()