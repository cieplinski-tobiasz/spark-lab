from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)

words = sc.textFile("README.md").flatMap(lambda x: x.split(" "))

result = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

for (key, value) in result.take(10):
    print(f"word={key}, count={value}")

