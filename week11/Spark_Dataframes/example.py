from pyspark.sql import SparkSession
from pyspark import SparkConf

# spark = SparkSession.builder \
#     .appName('SparkSession Example') \
#     .master("local[*]") \
#     .getOrCreate()
# OR

import os

spark_home = os.environ.get('SPARK_HOME', None)

print(spark_home)

# Spark Configuration
sparkConf = SparkConf()
sparkConf.set("spark.appName", "My First Application")
sparkConf.set("spark.master", "local[*]")

# Spark Session
spark = SparkSession.builder \
    .config(sparkConf) \
    .getOrCreate()

# Logic here


# Close spark session
spark.stop()