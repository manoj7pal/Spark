from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType, TimestampType


def init_spark():
    sparkConf = SparkConf()
    sparkConf.set('spark.app.name', 'Order Processing')
    sparkConf.set('spark.master', 'local[*]')

    spark = SparkSession.builder\
        .config(conf=sparkConf)\
        .getOrCreate()
    
    return spark

def load_customer_data(spark):
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

    customers_df = spark.read   \
            .format("csv")  \
            .option('header', 'true')  \
            .schema(schema=customer_schema) \
            .load('dataset/customers.csv')
    
    return customers_df

def load_orders_data(spark):
    order_schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("order_date", TimestampType(), True),
        StructField("order_customer_id", IntegerType(), True),
        StructField("order_status", StringType(), True)
    ])

    orders_df = spark.read  \
            .format("csv")   \
            .option('header', 'true')  \
            .schema(schema=order_schema)    \
            .option('dateFormat', 'yyyy-MM-dd HH:mm:ss.S')  \
            .load('dataset/orders.csv')
    
    return orders_df

def simple_join(spark, customers_df, orders_df):
    # Column Object Notation
    from pyspark.sql.functions import expr

    joined_df_col = customers_df.join(
                    other = orders_df,
                    on = orders_df['order_customer_id'] == customers_df['customer_id'],
                    how = 'inner'
                )\
                .withColumn('order_id', expr('coalesce(order_id, -1)')) \
                .orderBy('customer_id')
    
    # joined_df_col.show()

    # String Expression

    joined_df_str = customers_df.join(
                    other = orders_df,
                    on = orders_df['order_customer_id'] == customers_df['customer_id'],
                    how = 'inner'
                )\
                .selectExpr(
                    'coalesce(order_id, -1) as order_id', 
                    'customer_id',
                    'customer_fname', 
                    'customer_lname', 
                    'order_date', 
                    'order_status'
                )\
                .orderBy('customer_id')
    
    # joined_df_str.show()

    # Spark SQL way
    customers_df.createOrReplaceTempView('customers')
    orders_df.createOrReplaceTempView('orders')

    joined_df_sql = spark.sql("""
                        SELECT
                              coalesce(order_id, -1) as new_order_id,
                              customer_id,
                              customer_fname,
                              customer_lname,
                              order_date,
                              order_status
                            FROM customers as c 
                            INNER JOIN orders as o
                              ON c.customer_id = o.order_customer_id
                        """)
    joined_df_sql.show()



    

def join():
    '''
    There are 2 kinds of joins:
    1. Simple Join: Shuffle Sort Merge Join
    2. Broadcast Join
    '''

    # 1. init Spark
    spark = init_spark()

    # 2. Load customers and orders dataset
    customers_df = load_customer_data(spark)
    orders_df = load_orders_data(spark)

    # 3. Create Temp View
    simple_join(spark, customers_df, orders_df)

join()