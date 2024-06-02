'''
Given the input file where columns are stationId, timeOfTheReading, readingType, temperatureRecorded, and few other columns.
We need to find the minimum temperature for each station id.
Please do it using apache spark.
'''

from pyspark import SparkContext


def parse_weather_data(line):
    fields = line.split(",")
    station_id = fields[0]
    temp = float(fields[3])

    return station_id, temp


sc = SparkContext(sparkHome="local[*]", appName="MinTemperatureByStationId")
sc.setLogLevel('ERROR')

rdd = sc.textFile("../../dataset/assignment1_dataset2.csv")

# station_temp = rdd.filter(lambda line: "TMIN" in line).map(parse_weather_data)
station_temp = rdd.filter(lambda line: "TMIN" in line)\
                .map(lambda x: (x.split(',')[0], x.split(',')[3]))

station_min_temp = station_temp.reduceByKey(lambda x, y: min(x, y))
result = station_min_temp.collect()
print(type(result))

for a in result:
    print(a)
