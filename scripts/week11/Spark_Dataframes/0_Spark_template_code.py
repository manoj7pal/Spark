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
# ------------------------------------------
# Logic here


# ------------------------------------------
logging.info(f"{__file__} job successfully completed...")
spark.stop()