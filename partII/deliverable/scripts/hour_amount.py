from __future__ import print_function

import sys
import re
import string
from pyspark import SparkContext
from csv import reader
from operator import add
import time
import datetime

if __name__ == "__main__":
	
	sc = SparkContext()

	lines = sc.textFile(sys.argv[1], 1)

	# header = lines.first()
	# Remove the header
	lines = lines.mapPartitions(lambda x: reader(x))

	lines = lines.map(lambda x: (x[2])).map(lambda s: datetime.datetime.strptime(s, '%H:%M:%S').time())
	hour = lines.map(lambda x: (x.hour, 1)).reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[0])
	hour.collect()
	hour.map(lambda data: str(data[0]) + ',' + str(data[1]))
	hour.saveAsTextFile("hour_count.out")

	sc.stop()