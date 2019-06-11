from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster('local').setAppName('my app')
    sc = SparkContext(conf=conf)

    pt = sc.textFile('pan-tadeusz.txt')
    pan = pt.filter(lambda line: 'Pan' in line)
    tadeusz = pt.filter(lambda line: 'Tadeusz' in line)
    pan_or_tadeusz = pan.union(tadeusz)

    print(f'Słowo "Tadeusz" wystąpiło {tadeusz.count()} razy')
    print('5 przykładów:')

    for line in tadeusz.take(5):
        print(line)
