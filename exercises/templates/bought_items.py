from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster('local').setAppName('longest pt mention')
    sc = SparkContext(conf=conf)
    items = [
        ['milk', 'coffee', 'bread'],
        ['eggs', 'water'],
        ['coffee', 'water'],
        ['avocado']
    ]

    # your code here
