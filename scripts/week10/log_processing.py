from pyspark import SparkContext
from sys import stdin


def process_log_line(line):
    LOG_LEVEL = line.split(":")[0]
    return (LOG_LEVEL, 1)


if __name__ == "__main__":
    sc = SparkContext(master="local[*]", appName="LogProcessing")
    log_rdd = sc.textFile(name='../dataset/bigLog.txt')

    w_e_rdd = log_rdd.map(process_log_line)
    final = w_e_rdd.reduceByKey(lambda x, y: x + y).collect()

    for a in final:
        print(a)

stdin.readline()
