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
from pyspark.sql.functions import countDistinct, count, avg, sum
from datetime import datetime

spark = SparkSession.builder    \
    .appName("Spark UPF Application")   \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 1. Simple Aggregations:
'''
InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country
536378,,PACK OF 60 DINOSAUR CAKE CASES,24,01-12-2010 9.37,0.55,14688,United Kingdom

1. Load the file and create a DF using DF reader API
2. Simple aggregate - using all col object expression, string expression and Spark SQL way
    - total number of rows
    - total quantity
    - Avg Unit Price
    - No. of unique invoices
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
print('Column Object Notation')
invoice_df.select(
    count("*").alias('total_invoice_count'),
    sum("quantity").alias('total_quantity'),
    avg("unit_price").alias('average_unitprice'),
    countDistinct('invoice_id').alias('total_unique_invoice')
).show()
print('--'*40)

# String Expression
print('String Expression')
invoice_df.selectExpr(
    "count(*) as total_invoice_count",
    "sum(quantity) as total_quantity",
    "avg(unit_price) as average_unit_price",
    "count(distinct invoice_id) as total_unique_invoice"
).show() 
print('--'*40)

# SparkSQL
print('SparkSQL')
invoice_df.createTempView('invoice')
spark.sql('''
          SELECT 
			  count(*) as total_invoice_count,
			  sum(quantity) as total_quantity,
			  avg(unit_price) as average_unit_price,
			  count(distinct invoice_id) as total_unique_invoice
		    FROM invoice
          ''').show()

# print(invoice_df.show())
# print(invoice_df.printSchema())
print('--'*40)

spark.stop()
