'''
Transformations:
1. Low Level
    - map, filter, groupByKey()
    - using RDD's, some are even possible with DF and datasets

2. High Level
    - select, where groupBy
    - only possible with DF and Datasets.
'''

import re       
from datetime import datetime

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, StringType


def init_spark():
    sparkConf = SparkConf()
    sparkConf.set('spark.app.name', 'Order Processing')
    sparkConf.set('spark.master', 'local[*]')

    spark = SparkSession.builder\
        .config(conf=sparkConf)\
        .getOrCreate()
    
    return spark

def parser(line):
    pattern = re.compile(r"^(\S+)\s(\S+)\t(\S+)\,(\S+)$")
    match = pattern.match(line)

    if match:
        return ( 
                int(match.group(1)),
                datetime.strptime(match.group(2), "%Y-%m-%d"),
                int(match.group(3)),
                match.group(4)
                )
    else:
        return None

def low_level_transformations():
    spark = init_spark()
    spark.sparkContext.setLogLevel('ERROR')

    # Input - 1 2013-07-25	11599,CLOSED - RDD
    lines = spark.sparkContext.textFile('dataset/orders_new.csv')

    # Transformation
    parsed_lines = lines.map(parser).filter(lambda x: x is not None)

    # Convert rdd to Dataframe with a schema
    order_schema = StructType([
        StructField('order_id', IntegerType(), nullable=False),
        StructField('order_date', TimestampType(), nullable=False),
        StructField('customer_id', IntegerType(), nullable=False),
        StructField('order_status', StringType(), nullable=False)
    ])

    orders_df = spark.createDataFrame(data=parsed_lines, schema=order_schema).cache()
    # orders_df.show()

    # High Level Transformations
    # grouped_order = orders_df \
    #     .groupBy('order_status')    \
    #     .count()    \
    #     .orderBy('count', ascending=False) 
    
    # grouped_order.show()

    return orders_df

# -----------------------------------------------------------
# Referring columns of DF
# -----------------------------------------------------------


def ref_via_column_string_notation(orders_df):
    orders_df.select(
        'order_id', 
        'order_status'
        ).show()

from pyspark.sql.functions import col, column

def ref_via_column_object_notation(orders_df):
    orders_df.select(
        col('order_id'),
        column('order_status')
     ).show()

def referring_df_columns():
    orders_df = low_level_transformations()

    ref_via_column_string_notation(orders_df)
    ref_via_column_object_notation(orders_df)

# referring_df_columns()


# -----------------------------------------------------------
# Column Expressions: Calculate Field, New Column, etc
# - 
# -----------------------------------------------------------

from pyspark.sql.functions import expr

def via_expr(orders_df):
    orders_df.select(
        'order_id',
        'order_date',
        expr('concat(order_status, "_STATUS") as new_order_status')
    ).show(truncate=False)

def via_selectExpr(orders_df):
    orders_df.selectExpr(
        'order_id',
        'order_date',
        'concat(order_status, "_STATUS") as new_order_status'
    ).show(truncate=False)


def column_expressions():
    orders_df = low_level_transformations()
    
    via_expr(orders_df)
    via_selectExpr(orders_df)

column_expressions()