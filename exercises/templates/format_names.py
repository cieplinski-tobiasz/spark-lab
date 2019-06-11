from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster('local').setAppName('longest pt mention')
    sc = SparkContext(conf=conf)
    names = ['andy', 'jOhN', 'Peter', 'MiCHAel', '36453', '#@##']

    names_rdd = sc.parallelize(names)

    # your code here
