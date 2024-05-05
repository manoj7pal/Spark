import logging
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import desc
import sys

# Org level logger
# logging.getLogger(name="org").setLevel(level="ERROR")
logging.basicConfig(level=logging.INFO)

# Spark Configuration
sparkConf = SparkConf()
sparkConf.set('spark.app.name', "My First Application")
sparkConf.set('spark.master', 'local[*]')

# Spark Session
spark = SparkSession.builder \
    .config(conf = sparkConf) \
    .getOrCreate()

# spark Context level logger - Set LogLevel
# spark.sparkContext.setLogLevel("ERROR")

# Input - sample, with header, and inferSchema argument
orders_df = spark.read.csv(path="dataset/orders.csv", header=True, inferSchema=True)
print(f'Schema: {orders_df.printSchema()}')

# Basic Operations - repartition, where, select, groupby 
result_df = orders_df.repartition(numPartitions=4) \
            .where("order_customer_id > 10000") \
            .select("order_id", "order_customer_id") \
            .groupby("order_customer_id") \
            .count() \
            .orderBy("count", ascending=False)  # OR  .sort(desc("count"))

# First 20 rows
print(f'Customer Orders: {result_df.show(20)}')
logging.info(f"{__file__} job successfully completed...")

# sys.stdin.readline()
spark.stop()