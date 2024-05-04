from pyspark import SparkContext


def parse_data(data):
    fields = data.split('::')
    age, no_friends = int(fields[2]), int(fields[3])
    return age, no_friends


sc = SparkContext(sparkHome='local[*]', appName='FriendsByAge')

# Sr, Name, Age, no_friends - Find Avg Friends by Age
data = sc.textFile(name='../../dataset/friends_data.csv')
rdd = data.map(parse_data)  # (33, 385), (33, 400)
rdd2 = rdd.mapValues(lambda x: (x, 1))  # (33, (385,1)), (33, (400,1))
total_friends_by_age = rdd2.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))  # (33, (785,2))
avg_friends_by_age = total_friends_by_age.mapValues(lambda x: x[0] / x[1]).collect()

for i in avg_friends_by_age:
    print(i)

# rdd2 = rdd.mapValues(lambda x: (x,1))  # (33, (385,1))
# result = rdd2.reduceByKey(lambda x,y: x[0] )
