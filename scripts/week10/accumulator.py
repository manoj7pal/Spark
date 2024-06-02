from pyspark import SparkContext


def accumulate_long_values(x):
    global long_accumulator
    long_accumulator += x
    return long_accumulator


if __name__ == "__main__":
    sc = SparkContext(master="local[*]", appName="SparkAccumulator")
    rdd = sc.parallelize([1, 2, 3, 4, 5], numSlices=5)

    # Define and initialize accumulator -
    long_accumulator = sc.accumulator(0.0)

    rdd.map(accumulate_long_values).collect()

    print(f"Accumulated value: {long_accumulator.value}")
