from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext("local[*]", "wordcount")
    sc.setLogLevel("ERROR")

    input = sc.textFile(name="../../../../dataset/sample.txt")
    words = input.flatMap(lambda x: x.split(" "))
    words_count = words.map(lambda x: (x.lower(), 1))
    result = words_count.sortBy(lambda x: x[1], ascending=False).collect()

    for a in result:
        print(a)