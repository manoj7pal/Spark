import os

import findspark

print(findspark.init('C:\spark\spark-2.4.4-bin-hadoop2.7'))

spark_home = os.environ.get('SPARK_HOME', None)
print(spark_home)