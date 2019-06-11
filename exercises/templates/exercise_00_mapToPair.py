from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)

# Create RDD that holds pairs (key, value) where key is a string and value is its length
numbers = sc.parallelize(["one", "Spark", "Hadoop", "i"])
numberPairs = ???

for (key, value) in numberPairs.collect():
    print(f"key: {key}; value: {value}")
