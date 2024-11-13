'''
Grouping based on LOgging Levels(5) and MOnth(12)

debug, january, 7500
error, january , 50000
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
logs_schema = StructType([
    StructField('level', StringType(), nullable=False),
    StructField('datetime', TimestampType(), nullable=False)
])

# Sample Data Loading
# logs_df = spark.read    \
#             .format('csv')  \
#             .option('header', 'true')   \
#             .schema(schema=logs_schema) \
#             .option('dateFormat', 'yyyy-MM-dd HH:mm:ss')    \
#             .load('dataset/biglog_sample.csv')

# Original Data loading
logs_df = spark.read    \
            .format('csv')  \
            .option('header', 'true')   \
            .schema(schema=logs_schema) \
            .option('dateFormat', 'yyyy-MM-dd HH:mm:ss')    \
            .load('dataset/biglog_new.txt')


logs_df.createOrReplaceTempView('logs')

# spark.sql('''
#           SELECT 
#             level, 
#             count(datetime) as occurences,
#             collect_list(datetime) as list_occurences
#           FROM logs
#           GROUP BY level
#           ORDER BY level
#     ''').show(truncate=False)

result_df = spark.sql('''
                    SELECT 
                    level, 
                    date_format(datetime, 'MMMM') as month,
                    cast(first(date_format(datetime, 'MM')) as int) as month_number,
                    count(*) as occurrences
                    FROM logs
                    GROUP BY level, month
            ''')


# result_df.createOrReplaceTempView('new_logs')

# spark.sql('''
#         SELECT 
#           level,
#           month,
#           occurrences
#         FROM new_logs
#         ORDER BY month_number, level
#     ''').show(n=60, truncate=False)

# --------------------------------------------------------------
# PIVOT TABLE
  ## .pivot() --> This takes time - as Spark calculates distinct unique values. 
  #                 Hence we gone ahead and used .pivot(pivot_col='month', values=month_list) --> MORE OPTIMIZED
# --------------------------------------------------------------

# spark.sql('''
#         SELECT 
#           level,
#           date_format(datetime, 'MMMM') as month,
#           cast(date_format(datetime, 'MM') as int) as month_number
#         FROM logs
#     ''')    \
#         .groupBy('level')    \
#         .pivot('month_number')    \
#         .count()    \
#         .show(n=60, truncate=False)

month_list = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']

spark.sql('''
        SELECT 
          level,
          date_format(datetime, 'MMMM') as month
        FROM logs
    ''')    \
        .groupBy('level')    \
        .pivot(pivot_col='month', values=month_list)    \
        .count()    \
        .show(n=60, truncate=False)
    


spark.stop()