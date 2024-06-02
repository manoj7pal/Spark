from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, StringType

# Spark Configuration
sparkConf = SparkConf()
sparkConf.set('spark.appName', 'ExplicitSchemaTest')
sparkConf.set('spark.master', 'local[*]')

# Spark Session
spark = SparkSession.builder \
        .config(conf=sparkConf) \
        .getOrCreate()    

# Define Explicit Schema
'''
2 ways:
    1. Programmatic Approach
    2. DDL String Approach
'''

# 1. Programmatic Approach - PySpark Types
# orders_schema = StructType([
#     StructField('order_id', IntegerType(), nullable=False),
#     StructField('order_date', TimestampType(), nullable=False),
#     StructField('customer_id', IntegerType(), nullable=False),
#     StructField('order_status', StringType(), nullable=False),
# ])

# 2. DDL String Approach - Scala Types
orders_schema = "order_id INT, order_date TIMESTAMP, customer_id INT, order_status STRING"

# Use the schema to load the csv file
orders_df = spark.read \
    .schema(schema=orders_schema, ) \
    .csv(path='dataset/orders.csv', header=True)

print(orders_df.show())
print(orders_df.printSchema())
