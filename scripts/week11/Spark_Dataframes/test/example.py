import os
from pyspark.sql import SparkSession
from pyspark import SparkConf

# spark = SparkSession.builder \
#     .appName('SparkSession Example') \
#     .master("local[*]") \
#     .getOrCreate()
# OR

# Spark Configuration
# sparkConf = SparkConf()
# sparkConf.set("spark.appName", "My First Application")
# sparkConf.set("spark.master", "local[*]")
print('_____')


# Spark Session
spark = SparkSession.builder \
    .config(sparkConf) \
    .getOrCreate()

print(spark)


# Close spark session
spark.stop()