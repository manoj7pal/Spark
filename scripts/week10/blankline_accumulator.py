from pyspark import SparkContext


def count_blank_lines(line):
    global counter
    if len(line) == 0:
        counter += 1

    return counter


if __name__ == "__main__":
    sc = SparkContext(master="local[*]", appName="BlankLineAccumulator")
    rdd = sc.textFile(name="../dataset/blankLine.txt")

    # Driver Node: Accumulator variable creation
    counter = sc.accumulator(0)

    # Executor executes the function . and then the accumulator collects each executor output
    rdd.map(count_blank_lines).collect()

    # Fetch the accumulator variable value
    print(f"Number of blank lines: {counter.value}")