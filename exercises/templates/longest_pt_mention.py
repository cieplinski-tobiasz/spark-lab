import os.path

from pyspark import SparkConf, SparkContext

LAB_DIR = os.environ['SPARK_LAB_HOME'] if 'SPARK_LAB_HOME' in os.environ else os.path.join('..', '..')

if __name__ == '__main__':
    conf = SparkConf().setMaster('local').setAppName('longest pt mention')
    sc = SparkContext(conf=conf)
    pt_path = os.path.join(LAB_DIR, 'pan-tadeusz.txt')
    pt = sc.textFile(pt_path)

    # your code here
