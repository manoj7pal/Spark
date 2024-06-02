# import findspark
# print(findspark.init())
# print(findspark.find())
#
# # -----------------------------
#
from pyspark import SparkContext

sc = SparkContext(sparkHome='local[*]', appName='Test123')
#
# a = range(1,100,2)
#
# rdd = sc.parallelize(a)
# result = rdd.reduce(lambda x, y: x+y)
# print(f'Result: {result}')
#
# # -----------------------------
#
# input = sc.textFile('../dataset/customer_orders.csv')
# print(f'Count: {input.count()}')

a = (1, 2, 3, 4)
print(type(a))
print(sc.defaultParallelism)  # 16
print(sc.defaultMinPartitions)  # 2
