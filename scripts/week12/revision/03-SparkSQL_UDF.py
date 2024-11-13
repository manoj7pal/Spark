import re       
from datetime import datetime

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType


def init_spark():
    sparkConf = SparkConf()
    sparkConf.set('spark.app.name', 'Order Processing')
    sparkConf.set('spark.master', 'local[*]')

    spark = SparkSession.builder\
        .config(conf=sparkConf)\
        .getOrCreate()
    
    return spark

def load_new_df():
    # sumit,30,bangalore
    spark = init_spark()

    df_schema = "name string, age integer, city string"
    df = spark.read \
        .format("csv")  \
        .schema(df_schema)  \
        .load("dataset/assignment1_dataset1.csv")
    
    return df

def age_check(age):
    if age> 18:
        return 'Y'
    else:
        return 'N'

from pyspark.sql.functions import udf, col, expr

def via_udf_columnObjectNotation(df):
    # Create Spark UDF
    age_check_udf = udf(f=age_check, returnType=StringType())

    # Calling the function
    res_df = df.withColumn( 
                colName = "adult", 
                col = age_check_udf(col('age'))
            )

    print(res_df.show())

def via_udf_sql_expr(df):
    
    # Register UDF 
    spark = init_spark()
    spark.udf.register(f=age_check, name="age_check_udf", returnType=StringType())

    res_df = df.withColumn(
                colName = "adult",
                col = expr("age_check_udf(age)")
            )
    print(res_df.show())
    print('Registered UDF: ', [ func for func in spark.catalog.listFunctions() if func.name=='age_check_udf'][0])


def via_udf_spark_sql(df):
    
    # Register UDF in Spark Catalog
    spark = init_spark()
    spark.udf.register(f=age_check, name="age_check_udf", returnType=StringType())

    df.createOrReplaceTempView('people')
    spark.sql('''
              SELECT 
                name,
                age,
                city,
                age_check_udf(age) as is_adult
              FROM people
              ''').show()


def call_udf():
    df = load_new_df()
    
    # via_udf_columnObjectNotation(df)
    # via_udf_sql_expr(df)   
    via_udf_spark_sql(df)

call_udf()