import os.path

from pyspark import SparkConf, SparkContext

LAB_DIR = os.environ['SPARK_LAB_HOME'] if 'SPARK_LAB_HOME' in os.environ else os.path.join('..', '..')

if __name__ == '__main__':
    conf = SparkConf().setMaster('local').setAppName('my app')
    sc = SparkContext(conf=conf)
    pt_path = os.path.join(LAB_DIR, 'pan-tadeusz.txt')

    file = sc.textFile(pt_path)
    words = file.flatMap(lambda line: line.split())
    counts = words.map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y)
    counts.saveAsTextFile('pan-tadeusz-count')
