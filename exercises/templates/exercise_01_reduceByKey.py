from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)


numbers = sc.parallelize(["hej", "ho", "panda", "ho", "ho"])
numberPairs = numbers.map(lambda x: (x, len(x)))

# Create an RDD that consists of pairs of (key, value) where key is a word and value is a pair (sum of lengths of the key, count of times it appeared)
# Use mapValues and reduceByKey!
pairs = ???


# in avg values should give a length of a key since it's an average of lengths of the same words
avg = pairs.mapValues(lambda x: x[0] / x[1])

for (key, value) in avg.collect():
    print(f"key: {key}, value: {value}")

