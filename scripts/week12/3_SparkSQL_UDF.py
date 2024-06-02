# --assignment1_dataset1

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import expr, col, udf

spark = SparkSession.builder    \
    .appName("Spark UPF Application")   \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# schema
df_schema = "name string,age int,city string"

# Read Data
df = spark.read \
    .format('csv')  \
    .schema(df_schema)  \
    .load('./dataset/assignment1_dataset1.csv')

# print(df.show())
# print(df.printSchema())

# To set new column names
# new_df = df.toDF('new_name', 'new_age', 'new_city')
# print(new_df.printSchema())

# Spark UDF: Register and Usage
def check_age(age):
    if age > 18:
        return "Y"
    else:
        return "N"

#-------------------------------------------------------------------------------------
# UDF - For Programmatic DataFrame Operations - use pyspark.sql.functions.udf()
# - Using Column object notation
#-------------------------------------------------------------------------------------
check_age_ps_udf = udf(f=check_age, returnType=StringType())

print('PySpark UDF: Programmatic DataFrame way - used same as Python function')
res = df.withColumn(colName='is_adult', col = check_age_ps_udf(col('age')) )

print(res.show())

#-------------------------------------------------------------------------------------
# UDF - For SQL DataFrame Operations - use spark.udf.register()
# - EASY WAY
#-------------------------------------------------------------------------------------
print('PySpark UDF: SQL way - Extra step of registering')

# Registering Spark UDF, in Spark Catalog
spark.udf.register(f=check_age, name = "check_age_sql_udf", returnType=StringType())

# Diff ways of using the functions
# -----------------------------------------------
# With Column - expr
res = df.withColumn(colName='is_adult', col = expr("check_age_sql_udf(age)") )
print(res.show())

# OR

# df.selectExpr()
res = df.selectExpr('name',   \
          'age',    \
          'check_age_sql_udf(age) as is_adult' 
        )

print(res.show())

# OR 

# SQL Way - SQL String
print('PURE SQL WAY')

df.createTempView('people')
res = spark.sql('SELECT name, age, check_age_sql_udf(age) as is_adult, city from people')
print(res.show())

# -----------------------
# Spark Catalog - The function is registered in Spark Catalog
print(f"Spark.Catalog: {spark.catalog.listFunctions()}")


spark.stop()