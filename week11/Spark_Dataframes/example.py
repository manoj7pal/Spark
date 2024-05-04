import os
os.environ["JAVA_HOME"] = "C:/java/jdk-1.8"
os.environ["SPARK_HOME"] = "C:/spark"
import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").getOrCreate()
df=spark.read.options(delimiter=",", header=True).csv("../../dataset/chapters.csv")
df.show()


from pyspark.sql import SparkSession
from pyspark import SparkConf

# spark = SparkSession.builder \
#     .appName('SparkSession Example') \
#     .master("local[*]") \
#     .getOrCreate()
# OR

import os

spark_home = os.environ.get('SPARK_HOME', None)
java_home = os.environ.get('JAVA_HOME', None)
print(f'Spark Home: {spark_home}')
print(f'Java Home: {java_home}')

# Spark Configuration
sparkConf = SparkConf()
sparkConf.set("spark.appName", "My First Application")
sparkConf.set("spark.master", "local[*]")
print('_____')


# Spark Session
spark = SparkSession.builder \
    .config(sparkConf) \
    .getOrCreate()

print(spark)


# Close spark session
spark.stop()