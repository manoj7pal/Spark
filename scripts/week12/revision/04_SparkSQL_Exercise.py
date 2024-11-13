'''
Exercise:

1. Create a list from the input file orders.csv
2. From the list, create a DF - orderid, order_Date, customer_id, status
3. Convert orderdate field to Epoch timestamp - number of secopnds after 1st Jan 1970
4. Create a new column with name 'newid' and make sure it has unique id's
5. Drop duplicates - based on (orderdate, customer_id)
6. Drop orderid column
'''

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, monotonically_increasing_id


def init_spark():
    sparkConf = SparkConf()
    sparkConf.set('spark.app.name', 'Order Processing')
    sparkConf.set('spark.master', 'local[*]')

    spark = SparkSession.builder\
        .config(conf=sparkConf)\
        .getOrCreate()
    
    return spark

def load_data(spark):
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
    df_schema = "order_id int, order_date string, customer_id int, order_status string"
    df = spark.createDataFrame(data=sample_list, schema = df_schema)
    return df

def process_data(spark, df):
    df.createOrReplaceTempView('orders')
    res_df = spark.sql('''
                        SELECT
                            order_id,
                            monotonically_increasing_id()               as new_order_id,
                            to_date(order_date, 'yyyy-mm-dd')           as order_date,
                            unix_timestamp(order_date, 'yyyy-mm-dd')    as new_order_date,
                            customer_id,
                            order_status
                        FROM orders
                    ''')   \
                    .dropDuplicates(subset=['order_date', 'customer_id'])  \
                    .drop('order_id') \
                    .orderBy('new_order_id', ascending=True)
    
    print(res_df.show())   
    print(res_df.printSchema())

def run():
    spark = init_spark()
    df = load_data(spark)
    process_data(spark, df)

    spark.stop()

run()    