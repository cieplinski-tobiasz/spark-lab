import os.path

from pyspark.sql import SparkSession

LAB_DIR = os.environ['SPARK_LAB_HOME'] if 'SPARK_LAB_HOME' in os.environ else os.path.join('..')

if __name__ == '__main__':
    spark = SparkSession.builder.master('local').appName('ex 1').getOrCreate()

    cities = spark.read.csv(os.path.join(LAB_DIR, 'cities.csv'),
                            header=True, ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True)

    # RDD Solution
    cities_rdd = cities.rdd
    cities_in_state = cities.rdd \
        .map(lambda row: (row['State'], 1)) \
        .reduceByKey(lambda x, y: x + y)

    print(cities_in_state.collect())

    # DataFrame Solution
    cities \
        .groupby('State') \
        .agg({'City': 'count'}) \
        .show()
