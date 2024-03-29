import os.path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

LAB_DIR = os.environ['SPARK_LAB_HOME'] if 'SPARK_LAB_HOME' in os.environ else os.path.join('..')

if __name__ == '__main__':
    spark = SparkSession.builder.master('local').appName('ex 3').getOrCreate()

    cities = spark.read.csv(os.path.join(LAB_DIR, 'cities.csv'),
                            header=True, ignoreLeadingWhiteSpace=True, ignoreTrailingWhiteSpace=True)

    # RDD Solution
    cities_rdd = cities.rdd
    cities_count = cities.rdd \
        .map(lambda row: (row['State'], row['City'])) \
        .filter(lambda x: x[1].startswith('W')) \
        .groupByKey() \
        .mapValues(lambda x: len(x))

    print(cities_count.collect())

    # DataFrame Solution
    cities_count_df = cities \
        .filter(col('City').startswith('W')) \
        .groupby('State') \
        .agg({'City': 'count'}) \
        .show()
