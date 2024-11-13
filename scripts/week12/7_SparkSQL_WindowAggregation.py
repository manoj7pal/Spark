'''
Aggregate Transformations:
    1. Simple aggregations 
        - Single Row aggregations: count(), sum() etc from entire dataset
    2. Grouping aggregations 
        - Aggregations by group of 1 ore more columns
    3. Window aggregations 
        - Aggregations by partitions/window
        - Fixed size window - Last 7 days, Moving 7 day average etc
'''

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType
from pyspark.sql.functions import countDistinct, count, avg, sum, round
from pyspark.sql.window import Window
from datetime import datetime

spark = SparkSession.builder    \
    .appName("Spark UDF Application")   \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 2. Window Aggregations:
'''
InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country
536378,,PACK OF 60 DINOSAUR CAKE CASES,24,01-12-2010 9.37,0.55,14688,United Kingdom

1. Load the file and create a DF using DF reader API
2. Window aggregate - using all col object expression, string expression and Spark SQL way
    - Partition: Country 
    - Order based on week number 
        - Window Aggregation(From 1st to Current Row): calculate Moving Sum of invoice value 
'''
invoice_window_schema = StructType([
    StructField('country', StringType()),
    StructField('week_number', IntegerType()),
    StructField('num_invoices', IntegerType()),
    StructField('total_quantity', IntegerType()),
    StructField('invoice_value', FloatType())
])

invoice_df = spark.read     \
                .format("csv")  \
                .schema(schema=invoice_window_schema) \
                .option('header', 'true') \
                .load('dataset/windowdata.csv')

from pyspark.sql.window import Window

# Column Object Expression
window_specs = Window   \
    .partitionBy('country') \
    .orderBy('week_number') \
    .rowsBetween(start = Window.unboundedPreceding, end=Window.currentRow)

res = invoice_df.withColumn('running_total', round(sum('invoice_value').over(window_specs), 2) )

print(res.show())
print(res.printSchema())

# String Expression
res = invoice_df.selectExpr(
    'country',
    'num_invoices',
    'total_quantity',
    'invoice_value',
    '''round(
            sum(invoice_value) 
            over (
                PARTITION BY country 
                ORDER BY week_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
            , 2)
        as running_total'''
)

print(res.show())
print(res.printSchema())


# Spark SQL 
invoice_df.createOrReplaceTempView('invoices')

res = spark.sql("""
        SELECT
            country,
            num_invoices,
            total_quantity,
            invoice_value,
            round(
                sum(invoice_value) 
                OVER (PARTITION BY country 
                        ORDER BY week_number 
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                , 2) as running_total
            FROM  invoices
    """)

print(res.show())
print(res.printSchema())

spark.stop()