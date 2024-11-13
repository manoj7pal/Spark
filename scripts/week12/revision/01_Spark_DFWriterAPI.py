from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, FloatType, TimestampType


spark = SparkSession.builder    \
    .appName('Sample DF Writer Application')    \
    .master('local[*]') \
    .enableHiveSupport()    \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel('ERROR')

# 1. Schema Defintion
'''
    order_id,order_date,order_customer_id,order_status
    1,2013-07-25 00:00:00.0,11599,CLOSED
'''

orders_schema = StructType(
    [
        StructField('order_id', IntegerType(), nullable=True),
        StructField('order_date', TimestampType(), nullable=True),
        StructField('order_customer_id', IntegerType(), nullable=True),
        StructField('order_status', StringType(), nullable=True)
    ]
)

# 2. Data Loadiong
orders_df = spark.read  \
    .format('csv')  \
    .option('header', 'true') \
    .schema(schema=orders_schema)   \
    .option('dateFormat', 'yyyy-MM-dd HH:mm:ss')    \
    .load('dataset/orders.csv')
    
# 3. SparkSQL
orders_df.createOrReplaceTempView('orders')

completed_orders_df = spark.sql('''
                        SELECT * 
                        FROM orders
                        WHERE order_status = 'COMPLETE'
                    ''')

# 4. DF Writer API
'''
Save modes:
    1. append: Put the file in the existing folder
    2. overwrite: First delete the existing folder, then it will create a new folder
    3. errorIfExists: Will give error if outp[ut folder already exists
    4. ignore: if folder exists it will ignorethe writing process

Efficient Storage/Write ways:
1. Desired no of Files - .repartition()
2. Partitioning: .partitionBy()
3. Buvketing: .bucketBy(<no_partitions>, 'column_name')
4. Max Records Per File: .maxRecordsPerFile(2000)
'''
completed_orders_df.write \
    .format('parquet')  \
    .mode(saveMode='overwrite') \
    .option('path', 'output/spark_week12/revision/01')  \
    .save()

orders_df.write \
    .format('csv')  \
    .partitionBy('order_status')    \
    .mode(saveMode='overwrite') \
    .option('path', 'output/spark_week12/revision/02')  \
    .save()

orders_df.write \
    .format('csv')  \
    .option('maxRecordsPerFile', 2000)  \
    .mode(saveMode='overwrite') \
    .option('path', 'output/spark_week12/revision/02')  \
    .save()

# Persist SparkSQL table - Managed Table
spark.sql("CREATE DATABASE IF NOT EXISTS retail")

orders_df.write \
    .mode(saveMode='overwrite') \
    .bucketBy(4, 'order_customer_id')   \
    .saveAsTable('retail.orders')

print(spark.catalog.listTables(dbName='retail'))
print(spark.catalog.listDatabases())

spark.stop()