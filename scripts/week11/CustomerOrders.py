from pyspark import SparkContext, StorageLevel
from sys import stdin

sc = SparkContext(sparkHome='local[*]', appName='CustomerOrders')

# CustomerID, ProductId, AmountSpent
cust_order_rdd = sc.textFile(name='../dataset/customer_orders.csv')

# Transformation
customer_spent = cust_order_rdd.map(lambda x: (x.split(',')[0], float(x.split(',')[2])))

customer_totalSpent = customer_spent.reduceByKey(lambda x, y: x + y).cache()
# customer_totalSpent = customer_spent.reduceByKey(lambda x, y: x + y).persist(StorageLevel.MEMORY_AND_DISK)

customer_gt_5000 = customer_totalSpent.filter(lambda x: x[1] > 5000)
sorted_final = customer_gt_5000.sortBy(lambda x: x[1], ascending=False)

# Save as text file - Action function
sorted_final.collect()

print(sorted_final.toDebugString())

stdin.readline()