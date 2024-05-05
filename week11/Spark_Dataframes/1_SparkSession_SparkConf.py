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
    .getOrCreate()

# Set LogLevel
spark.sparkContext.setLogLevel("WARN")

# Input - sample, with header, and inferSchema argument
orders_df = spark.read.csv(path="dataset/orders.csv", header=True, inferSchema=True)
print(f'Schema: {orders_df.printSchema()}')
print(f'Customer Orders: {orders_df.show(20)}')

sys.stdin.readline()
spark.stop()