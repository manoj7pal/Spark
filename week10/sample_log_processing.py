from pyspark import SparkContext

sc = SparkContext(master="local[*]", appName="SampleLogProcessor")
log = [
    "ERROR: Thu Jun 04 10:37:51 BST 2015",
    "WARN: Sun Nov 06 10:37:51 GMT 2016 ",
    "WARN: Mon Aug 29 10:37:51 BST 2016 ",
    "ERROR: Thu Dec 10 10:37:51 GMT 2015",
    "ERROR: Fri Dec 26 10:37:51 GMT 2014",
    "ERROR: Thu Feb 02 10:37:51 GMT 2017",
    "WARN: Fri Oct 17 10:37:51 BST 2014 ",
    "ERROR: Wed Jul 01 10:37:51 BST 2015",
]
# Create a RDD from a List
log_rdd = sc.parallelize(log)

log_level_rdd = log_rdd.map(lambda x: (x.split(":")[0], 1))
final = log_level_rdd.reduceByKey(lambda x, y: x + y).collect()

print(f'Default Parallelism: {sc.defaultParallelism}')
print(f'No. of Partitions in RDD: {log_rdd.getNumPartitions()}')
print(f'Default Minimum Partitions in RDD: {sc.defaultMinPartitions}')

for a in final:
    print(a)
