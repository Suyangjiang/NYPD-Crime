from csv import reader
from pyspark import SparkContext
from operator import add
import sys
import datetime


def unityear(x):
        dt = datetime.datetime.strptime(x[1].strip(), "%m/%d/%Y")
        y = dt.month
        result = x[10].strip()
        return ((result, y), 1)

def stat(x):
        result = x[0][0]
        year = x[0][1]
        return "%s, %s, %d" % (result, year, x[1])

if __name__ == "__main__":
        sc = SparkContext()
        data = sc.textFile(sys.argv[1], 1)

        data = data.mapPartitions(lambda x: reader(x))\
                        .map(unityear)\
                        .reduceByKey(add)\
                        .filter(lambda x: x[0])\
                        .map(stat)
        data.saveAsTextFile("state_month_amount.out")

        sc.stop()