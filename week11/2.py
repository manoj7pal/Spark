from pyspark import SparkContext
from sys import stdin


def map_func(partition):
    return [x ** 2 for x in partition]


sc = SparkContext(master="local[*]", appName="MapPartitions")
rdd = sc.parallelize([1, 2, 3, 4, 5], 2)  # 2 partitions
squared_rdd = rdd.mapPartitions(map_func)

for i in squared_rdd.collect():
    print(i)

stdin.readline()
