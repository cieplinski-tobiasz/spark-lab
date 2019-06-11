from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)

nums = sc.parallelize([("hej", 12), ("ho", 1), ("panda", 2), ("ho", 1), ("ho", 2)])

sumCount = nums.combineByKey(
        (lambda x: (x, 1)), # defines what to do with the first element in the partition
        (lambda x, y: (x[0] + y, x[1] + 1)), # function combines acc and next element
        (lambda x, y: (x[0] + y[0], x[1] + y[1]))) # function combines different partitions of data


for (key, value) in sumCount.mapValues(lambda value: value[0] / value[1]).collect():
    print(f"key: {key}; value: {value}")
