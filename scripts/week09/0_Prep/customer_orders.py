from pyspark import SparkContext

sc = SparkContext (sparkHome="local[*]", appName="customer_orders")

# CustomerID, ProductId, AmountSpent
rdd1 = sc.textFile(name="../../dataset/customer_orders.csv")
rdd2 = rdd1.map(lambda x: ( x.split(",")[0], float(x.split(",")[2])) )
rdd3 = rdd2.reduceByKey(lambda x,y: x+y)
rdd4 = rdd3.sortBy(lambda x: x[1], ascending=False)

for i in rdd4.collect():
    print(i)

