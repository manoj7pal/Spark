from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext("local[*]", "wordcount")
    sc.setLogLevel("ERROR")

    input = sc.textFile(name="../../../dataset/sample.txt")
    words = input.flatMap( lambda x: x.split(" ") )
    word_counts = words.map( lambda x: x.lower() )

    # countByValue: Action Function --> dictionary
    final_count = word_counts.countByValue()

    print(final_count)