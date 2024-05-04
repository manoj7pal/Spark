from pyspark import SparkContext

sc = SparkContext(sparkHome="local[*]", appName='movie_ratings')

# MovieId, CustomerId, Ratings, UnixTimeStamp
lines = sc.textFile(name="../../dataset/movie_data.data")
ratings = lines.map(lambda x: (x.split("\t")[2],1) )
result = ratings.reduceByKey(lambda x,y: x+y).collect()

for a in result:
    print(a)

