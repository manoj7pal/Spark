import logging
from pyspark.sql import SparkSession
from pyspark.sql import Row
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
# ------------------------------------------
''' Generalized Data Ingestion way
## 3 read modes:
    a. PREMISSIVE (default) : It sets all the fields to NULL when it encounters a corrupted record. A new column `_corrupt_record` holds the bad records
    b. DROPMALFORMED        : Ignores the malformed record
    c. FAILFAST             : Exception is raised, if malformed record is encountered. 

## Default format is PARQUET - most efficient file formats

## Schema can be:
    a. INFER
    b. IMPLICIT (File formats: AVRO, PARQUET, HDF5)
    c. EXPLICIT (Types)
'''
df1 = spark.read \
        .format(source="csv") \
        .load(path = 'dataset/orders.csv', header = True, inferSchema=False, mode='PERMISSIVE')

df2 = spark.read \
    .format('parquet') \
    .load(path='dataset/users.parquet')

print(df2.show(truncate=False))  # truncate=False --> do not runcate the row value without truncating it with '...'
print(df2.printSchema())

# ------------------------------------------
logging.info(f"{__file__} job successfully completed...")
spark.stop()