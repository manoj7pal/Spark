# PySpark module - SparkContext, entry point in the Spark Cluster
from pyspark import SparkContext
from sys import stdin

if __name__ == "__main__":
    sc = SparkContext("local[*]", "wordcount")
    sc.setLogLevel("ERROR")

    input = sc.textFile("../../../dataset/sample.txt")

    # One row will give multiple output rows
    words = input.flatMap(lambda x: x.split(" "))
    word_counts = words.map(lambda x: (x,1) )
    final_count = word_counts.reduceByKey(lambda x,y: x+y)

    result = final_count.collect()

    for a in result:
        print(a)
else:
    print("Not executed directly")

stdin.readline()