from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


# Spark Configuration
sparkConf = SparkConf()
sparkConf.set('spark.appName', 'Assignment_Week11')
sparkConf.set('spark.master', 'local[*]')

# Spark Session
spark = SparkSession.builder \
    .config(conf = sparkConf) \
    .getOrCreate()

# set LogLevel
sc = spark.sparkContext
sc.setLogLevel('ERROR')

# Explicit Schema
win_schema = StructType([
    StructField('country', StringType(), nullable=False),
    StructField('week_num', IntegerType(), nullable=False),
    StructField('num_invoices', IntegerType(), nullable=False),
    StructField('total_quantity', IntegerType(), nullable=False),
    StructField('invoice_value', FloatType(), nullable=False)
])

# Load Data
win_df = spark.read \
    .format('csv') \
    .schema(schema=win_schema) \
    .option('header', False) \
    .option('path', 'dataset/windowdata.csv') \
    .load()

# Write to a parquet and avro file - Parquet Writer API
win_df.write \
    .parquet(path='output/windows_parquet_output', mode='overwrite', partitionBy=['country', 'week_num'])

# Generalized Writer API
win_df.write \
    .format('avro') \
    .mode('overwrite') \
    .partitionBy('country') \
    .save(path = 'output/windows_avro_output')

print(win_df.show())
print(win_df.printSchema())

spark.stop()
