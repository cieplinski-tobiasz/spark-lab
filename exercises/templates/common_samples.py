from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster('local').setAppName('longest pt mention')
    sc = SparkContext(conf=conf)
    nums = list(range(1000))

    nums_rdd = sc.parallelize(nums)

    # your code here
    # tip: use seeds for reproducibility
