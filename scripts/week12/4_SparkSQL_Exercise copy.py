'''
Exercise:

1. Create a list from the input file orders.csv
2. From the list, create aDF - orderid, order_Date, customer_id, status
3. Convert orderdate field to Epoch timestamp - number of secopnds after 1st Jan 1970
4. Create a new column with name 'newid' and make sure it has unique id's
5. Drop duplicates - based on (orderdate, customer_id)
6. Drop orderid column
'''

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DateType
from pyspark.sql.functions import expr, col, udf, unix_timestamp, monotonically_increasing_id
from datetime import datetime

spark = SparkSession.builder    \
    .appName("Spark UPF Application")   \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 1. Create a list from the input file orders.csv
sample_list = [
    [1,'2013-07-25', 11599, 'CLOSED'       ],
    [2,'2013-07-25', 256,'PENDING_PAYMENT'  ],
    [3,'2013-07-25', 12111,'COMPLETE'       ],
    [4,'2013-07-25', 8827, 'CLOSED'        ],
    [5,'2013-07-25', 11318,'COMPLETE'       ],
    [6,'2013-07-25', 7130,'COMPLETE'        ],
    [7,'2013-07-25', 4530,'COMPLETE'        ],
    [8,'2013-07-25', 2911,'PROCESSING'      ],
    [9,'2013-07-25', 5657,'PENDING_PAYMENT' ],
    [10,'2013-07-25', 5648,'PENDING_PAYMENT' ],
    [11,'2013-07-25', 918,'PAYMENT_REVIEW'   ],
    [12,'2013-07-25', 1837, 'CLOSED'        ],
    [12,'2013-07-25', 1837, 'CLOSED'        ]
]

# 2. From the list, create aDF - orderid, order_Date, customer_id, status
# orders_df = spark.createDataFrame(sample_list, ).toDF('order_id', 'order_date', 'customer_id', 'order_status')
orders_df = spark.createDataFrame(sample_list, schema = "order_id int,order_date string,customer_id int,order_status string" )

# 3. Convert orderdate field to Epoch timestamp - number of seconds after 1st Jan 1970
# orders_df = orders_df.withColumn('order_date', unix_timestamp(col('order_date'), format="yyyy-mm-dd"))    #   OR
# orders_df = orders_df.selectExpr("order_id", "unix_timestamp(order_date, 'yyyy-mm-dd') as order_date", "customer_id", "order_status" )    #   OR

orders_df.createTempView('orders')
res = spark.sql('''
                SELECT 
                    order_id,
                    monotonically_increasing_id() as new_order_id,
                    order_date,
                    unix_timestamp(order_date, 'yyyy-mm-dd') as new_order_date, 
                    customer_id,    
                    order_status    
                FROM orders
                ''' )   \
                .dropDuplicates(['order_date', 'customer_id'])  \
                .drop("order_id")   \
                .orderBy('new_order_id', ascending=True)


print(res.show())
print(res.printSchema())

spark.stop()