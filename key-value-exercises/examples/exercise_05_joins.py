# Joining data together is probably one of the most
# common operations on a pair RDD, and we have a full range of options including
# right and left outer joins, cross joins, and inner joins.

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)


rdd1 = sc.parallelize([(1,2), (3,4), (3,6)])
rdd2 = sc.parallelize([(3,9)])

# inner join
inner = rdd1.join(rdd2)
print("inner join")
for (key, value) in inner.collect():
    print(f"key: {key} with values: {value}")


leftOuter = rdd1.leftOuterJoin(rdd2)
print("Left outer join")
for (key, value) in leftOuter.collect():
    print(f"key: {key} with values: {value}")
