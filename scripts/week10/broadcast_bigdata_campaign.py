from pyspark import SparkContext
import numpy as np


# Broadcast Join function
def load_boring_words():
    boring_words = set()
    np_boringwords = np.genfromtxt(fname="../dataset/boringwords.txt", dtype='str')

    for word in np_boringwords:
        boring_words.add(word.lower())

    return boring_words


if __name__ == '__main__':
    # Find top 10 most words on which money are spent, in the Ad Campaign
    sc = SparkContext(sparkHome="local[*]", appName="Bigdata Campaign Analytics")
    input_rdd = sc.textFile(name="../dataset/bigdata_campaign.csv")

    # Broadcast variable to the cluster
    broadcast_boring_words = sc.broadcast(load_boring_words())

    # Value as Key, key as Value
    ppc_word = input_rdd.map(lambda x: (float(x.split(",")[10]), x.split(",")[0]))

    # Converted the String and mapped each value with Key using flatMapValues - with Key
    ppc_flatmap_word = ppc_word.flatMapValues(lambda x: x.split(" "))

    # Replace key with value, and Sanitizing - lower the key
    word_ppc = ppc_flatmap_word.map(lambda x: (x[1].lower(), x[0]))

    # Use the broadcast variable to filter out boring words
    filtered_word_ppc = word_ppc.filter(lambda x: x[0] not in broadcast_boring_words.value)
    result = filtered_word_ppc.reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[1], ascending=False)

    # print(f"Broadcast Value: { 'in' in broadcast_boring_words.value}")
    print(result.take(20))
