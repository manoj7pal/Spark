from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import col, column
import re
from datetime import datetime

def parser(line):
    pattern = re.compile(r"^(\S+)\s(\S+)\t(\S+)\,(\S+)$")
    match = pattern.match(line)

    if match:
        return ( 
                int(match.group(1)),
                datetime.strptime(match.group(2), "%Y-%m-%d"),
                int(match.group(3)),
                match.group(4)
                )
    else:
        return None


# Spark Configuration
sparkConf = SparkConf()
sparkConf.set('spark.app.name', "Orders Processing")
sparkConf.set('spark.master', 'local[*]')

# Spark Session
spark = SparkSession.builder \
    .config(conf = sparkConf) \
    .getOrCreate()

# Load an unstructured foile in an rdd
lines = spark.sparkContext.textFile('dataset/orders_new.csv')
print(type(lines)) # RDD

# Parse the lines - give them structure
parsed_lines = lines.map(parser).filter(lambda x: x is not None)

# Define the explicit schema - if required
orders_schema = StructType([
    StructField('order_id', IntegerType(), nullable=False),
    StructField('order_date', TimestampType(), nullable=False),
    StructField('order_customer_id', IntegerType(), nullable=False),
    StructField('order_status', StringType(), nullable=False)
])

# Convert RDD to DF - cached
orders_df = spark.createDataFrame(data=parsed_lines, schema=orders_schema).cache()

print(type(orders_df))
print(orders_df.show())
print(orders_df.printSchema())

grouped_order = orders_df   \
    .select(column('order_status'))   \
    .groupBy('order_status')   \
    .count()    \
    .orderBy('count', ascending=False)

print(grouped_order.show())

spark.stop()