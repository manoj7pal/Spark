'''
Given an Input data set with name, age and city
- if age > 18 add a new column that’s populated with ‘Y’ else ‘N’
'''

from pyspark import SparkContext


def parse_csv_line(line):
    fields = line.split(",")
    name = fields[0]
    age = int(fields[1])
    city = fields[2]

    age_category = "Y" if age > 18 else "N"
    return name, age, city, age_category


sc = SparkContext(sparkHome="local[*]", appName="isAdult?")
rdd = sc.textFile(name="../../dataset/assignment1_dataset1.csv")
data_with_age_category_rdd = rdd.map(parse_csv_line)

result = data_with_age_category_rdd.collect()
for record in result:
    print(record)
