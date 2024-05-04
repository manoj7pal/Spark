import os

import findspark

spark_home = os.environ.get('SPARK_HOME', None)
java_home = os.environ.get('JAVA_HOME', None)

print(f'Spark Home: {spark_home}')
print(f'Java Home: {java_home}')