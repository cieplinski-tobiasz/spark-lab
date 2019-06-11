from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)


numbers = sc.parallelize([1, 2, 3, 4 , 6, 7, 8, 9, 0])
numbers2 = sc.parallelize([11, 12, 14, 15, 16, 17, 18, 19, 10])

# groupBy applies provided function to each element and groups it by the result
numbersOddEven = numbers.groupBy(lambda x: x % 2 == 0)
numbersOddEven2 = numbers2.groupBy(lambda x: x % 2 == 0)

for (key, value) in numbersOddEven.collect():
    print(f"key: {key} and its  values:")
    for el in value:
        print(el)

# combines two RDDs; return iterables
coGrouped = numbersOddEven.cogroup(numbersOddEven2)

for (key, values) in coGrouped.collect():
    print(f"key: {key} and more of its values:")
    for el in values:
        print(el)

