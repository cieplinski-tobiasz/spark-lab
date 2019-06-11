from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)


numbers = sc.parallelize([2, 3, 3, 5])
numberPairs = numbers.map(lambda x: (x, x + 1))

for (key, value) in numberPairs.collect():
    print(f"key: {key}; value: {value}")


