from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)


numbers = sc.parallelize(["hej", "ho", "panda", "ho", "ho"])
numberPairs = numbers.map(lambda x: (x, len(x)))
 
avg = numberPairs.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

for (key, value) in avg.collect():
    print(f"key: {key}, value: {value}")

