from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType


def init_spark():
    sparkConf = SparkConf()
    sparkConf.set('spark.app.name', 'Order Processing')
    sparkConf.set('spark.master', 'local[*]')

    spark = SparkSession.builder\
        .config(conf=sparkConf)\
        .getOrCreate()
    
    return spark

def get_schema():
    '''
    InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country
    536378,,PACK OF 60 DINOSAUR CAKE CASES,24,01-12-2010 9.37,0.55,14688,United Kingdom
    '''

    invoice_schema = StructType([
        StructField('invoice_no', IntegerType(), nullable=False),
        StructField('stock_code', IntegerType(), nullable=True),
        StructField('description', StringType(), nullable=True),
        StructField('quantity', IntegerType(), nullable=True),
        StructField('invoice_date', DateType(), nullable=True),
        StructField('unit_price', FloatType(), nullable=True),
        StructField('customer_id', IntegerType(), nullable=True),
        StructField('country', StringType(), nullable=True)
    ])

    return invoice_schema

def load_data(spark):
    invoice_schema = get_schema()
    df = spark.read\
        .format('csv') \
        .schema(invoice_schema) \
        .option('dateFormat', 'dd-mm-yyyy')  \
        .option('header', 'true') \
        .load('dataset/order_data.csv')
    
    print('Schema: ', df.printSchema())   
    return df

# Scalar Aggregations - returns single value
def simple_aggregations(spark):
    spark.sql(
        '''
            SELECT
                count(*)                    as total_number_of_rows,
                sum(quantity)               as total_quantity,
                avg(unit_price)             as avg_unit_price,
                count(distinct invoice_no)  as number_of_unique_invoices
            FROM orders
        '''
    ).show()

# Return groups
def grouping_aggregations(spark):
    spark.sql(
    '''
        SELECT
            invoice_no,
            country,
            sum(quantity)                           as total_quantity,
            round(sum(quantity * unit_price), 2)    as total_invoice_value
        FROM orders
        GROUP BY invoice_no,country
        ORDER BY total_invoice_value DESC
    '''
    ).show()

# Window groupings
def window_aggregations(spark):
    '''
    Spain,49,1,67,174.72

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
    
    # Spark SQL 
    invoice_df.createOrReplaceTempView('invoices')

    spark.sql("""
            SELECT
                country,
                week_number,
                ROUND(
                    SUM(invoice_value)
                        OVER (
                            PARTITION BY country
                            ORDER BY week_number
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        )
                    , 2) as weekly_running_invoice_value
            FROM invoices
            ORDER BY country, week_number
        """).show()

def run():
    spark = init_spark()

    df = load_data(spark)
    df.createOrReplaceTempView('orders')

    # simple_aggregations(spark)
    # grouping_aggregations(spark)
    window_aggregations(spark)

    spark.stop()

run()
