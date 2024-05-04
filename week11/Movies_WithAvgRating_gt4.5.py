"""
Q. Find all movies with avg rating > 4.2
    - Atleast 100 people should have rated the movie
"""

from pyspark import SparkContext
from sys import stdin

sc = SparkContext(sparkHome='local[*]', appName='HighlyRatedMovies')
sc.setLogLevel(logLevel='ERROR')

raw_ratings = sc.textFile(name='../dataset/ratings.dat')

trans_ratings = raw_ratings.map(lambda x: (x.split('::')[1], x.split('::')[2]))  # movie_id, ratings (1193, 5) (1193, 3) (1193, 4)
agg_map_ratings = trans_ratings.mapValues(lambda x: (float(x), 1.0))  # Since average is a float value (1193,(5.0,1.0)) (1193,(3.0,1.0)) (1193,(4.0,1.0))
agg_ratings = agg_map_ratings.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))  # (1193,(12.0, 3.0))
filtered_ratings = agg_ratings.filter(lambda x: x[1][1] >= 100.0)  # (1193,(12.0.0, 3.0)) # 100 people rated
avg_movies_gt_45 = filtered_ratings.map(lambda x: (x[0], x[1][0]/x[1][1]))\
                        .filter(lambda x: x[1] > 4.2)  # (1193,12.0.0/3.0)  #(101, 4.7)

# ----------------------------------------------------
movies_rdd = sc.textFile(name='../dataset/movies.dat')
movie_names = movies_rdd.map(lambda x: (x.split('::')[0], x.split('::')[1]))

mapped_movies = avg_movies_gt_45.join(movie_names)
                        # .sortBy(lambda x: x[1][0], ascending=False)      # (101, (4.7, ToyStory) )
final = mapped_movies.map(lambda x: x[1][1])

print(f'Movie List')
print('---' * 60)
for movie in final.collect():
    print(f'{movie}')

stdin.readline()