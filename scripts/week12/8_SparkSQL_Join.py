'''
Customers
---------
customer_id,customer_fname,customer_lname,customer_email,customer_password,customer_street,customer_city,customer_state,customer_zipcode
1,Richard,Hernandez,XXXXXXXXX,XXXXXXXXX,6303 Heather Plaza,Brownsville,TX,78521

Order Data
----------
order_id,order_date,order_customer_id,order_status
1,2013-07-25 00:00:00.0,11599,CLOSED

Join column: customer id

'''

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType, TimestampType
from pyspark.sql.functions import countDistinct, count, avg, sum, round, expr
from pyspark.sql.window import Window
from datetime import datetime

spark = SparkSession.builder    \
    .appName("Spark UPF Application")   \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ----------------------------------------
# DATA LOADING
# ----------------------------------------
customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_fname", StringType(), True),
    StructField("customer_lname", StringType(), True),
    StructField("customer_email", StringType(), True),
    StructField("customer_password", StringType(), True),
    StructField("customer_street", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True),
    StructField("customer_zipcode", StringType(), True)  # Zip codes as String
])


order_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("order_date", TimestampType(), True),
    StructField("order_customer_id", IntegerType(), True),
    StructField("order_status", StringType(), True)
])

customers_df = spark.read   \
            .format("csv")  \
            .option('header', 'true')  \
            .schema(schema=customer_schema) \
            .load('dataset/customers.csv')

 

# print(customers_df.show())
# print(orders_df.show())

# ----------------------------------------
# JOIN:
'''
    - how = inner, outer(full outer join), left, right 
'''
# ----------------------------------------

# 1. Column Object Notation
joined_df = orders_df.join(other = customers_df, 
                           on = orders_df["order_customer_id"]==customers_df["customer_id"], 
                           how = "inner" )  \
                            .withColumn('order_id', expr('coalesce(order_id, -1)'))    \
                            .select('order_id', 'customer_id', 'customer_fname', 'customer_lname', 'order_date', 'order_status')    \
                            .orderBy('customer_id', ascending=True)

# print(joined_df.show())

# String Expression

joined_df = orders_df.join(
                other = customers_df,
                on = customers_df['customer_id']==orders_df['order_customer_id'],
                how = "inner")   \
                .selectExpr(
                    'coalesce(order_id, -1) as order_id', 
                    'customer_id',
                    'customer_fname', 
                    'customer_lname', 
                    'order_date', 
                    'order_status'
                )   \
                .orderBy('order_id', ascending=True)

# Spark SQL 
customers_df.createOrReplaceTempView('customers')
orders_df.createOrReplaceTempView('orders')

sql_joined_df = spark.sql("""
                SELECT
                      coalesce(o.order_id, -1) as order_id,
                      c.customer_id,
                      c.customer_fname,
                      c.customer_lname,
                      o.order_date,
                      o.order_status
                    FROM customers c
                      INNER JOIN  orders o
                      ON c.customer_id = o.order_customer_id
                    ORDER BY order_id 
            """)

print(sql_joined_df.show())

spark.stop()