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
from pyspark.sql.functions import countDistinct, count, avg, sum, expr
from datetime import datetime

spark = SparkSession.builder    \
    .appName("Spark UPF Application")   \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 2. Grouping Aggregations:
'''
InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country
536378,,PACK OF 60 DINOSAUR CAKE CASES,24,01-12-2010 9.37,0.55,14688,United Kingdom

1. Load the file and create a DF using DF reader API
2. Grouping aggregate - using all col object expression, string expression and Spark SQL way
    - Group: Country and Invoice_id
        - total quantity
        - sum of invoice value: sum(quantity) * unit_price
'''
invoice_schema = StructType([
    StructField('invoice_id', IntegerType()),
    StructField('stock_code', StringType()),
    StructField('description', StringType()),
    StructField('quantity', IntegerType()),
    StructField('invoice_date', StringType()),
    StructField('unit_price', FloatType()),
    StructField('customer_id', IntegerType()),
    StructField('country', StringType()),
])

invoice_df = spark.read     \
                .format("csv")  \
                .schema(schema=invoice_schema) \
                .option('header', 'true') \
                .load('dataset/order_data.csv')

# Column Object Expression
res = invoice_df    \
    .groupBy("country", "invoice_id")   \
    .agg(sum("quantity"), sum(expr("quantity * unit_price")))   \
    .toDF('country', 'invoice_id', 'total_quantity', 'total_invoice_value') 

print(res.show())
print(res.printSchema())

# String Expression
res = invoice_df    \
    .groupBy('country', 'invoice_id')    \
    .agg(
        expr("sum(quantity) as total_quantity"),
        expr("sum(quantity * unit_price) as total_invoice_values")
         )

print(res.show())
print(res.printSchema())

# Spark SQL 
invoice_df.createOrReplaceTempView('invoices')

res = spark.sql('''
        SELECT 
          country,
          invoice_id,
          sum(quantity) as total_quantity,
          sum(quantity * unit_price) as total_invoice_value
        FROM invoices
        WHERE invoice_id IS NOT NULL
        GROUP BY country, invoice_id
        ORDER BY country, total_invoice_value DESC
''')

print(res.show())
print(res.printSchema())

spark.stop()