from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)


numbers = sc.parallelize([(10, "a"), (1, "b"), (2, "c"), (3, "d"), (2, "e"), (12, "f"), (12, "g")])

sortRdd = numbers.sortByKey(ascending=True, numPartitions=None, keyfunc = lambda x: -x)

for (key, value) in sortRdd.collect():
    print(f"key: {key}; value: {value}")
