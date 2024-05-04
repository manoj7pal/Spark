from pyspark import SparkContext

sc = SparkContext(master="local[*]", appName="Week10_Assignment")

# 0. Data Loading

# Views csv files - userId,chapterId,dateAndTime
csv_files = ["../dataset/views1.csv", "../dataset/views2.csv", "../dataset/views3.csv"]
view_files = [sc.textFile(file) for file in csv_files]
views_rdd = sc.union(view_files)

# Chapters - chapterId,courseId
chapters_rdd = sc.textFile(name="../dataset/chapters.csv")

# ------------------------------------------------

# 1. How many Chapters are there per course ?
# SELECT COUNT(CHAPTER_ID) FROM...
# GROUP BY COURSE_ID
# Key: Course ID, Value: SUM(Distinct Chapter_id)

course_chapter_count = chapters_rdd.map(lambda x: (int(x.split(',')[1]), 1))
course_chapter_count = course_chapter_count.reduceByKey(lambda x, y: x + y) \
                                        .sortBy(lambda x: x[1], ascending=False) \
                                        .collect()

for result in course_chapter_count:
    print(result)

# 2. Produce a ranking chart detailing based on which are the most popular courses by score
    '''
    1. Create a RDD - userId, ChapterId, CourseId, TotalChapters
    2. Remove Duplicates, if any
    3. Pair RDD - Key: UserId, Value: ()
    '''
