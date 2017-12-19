from __future__ import print_function

import sys
import re
import string
from pyspark import SparkContext
from csv import reader
from operator import add
from datetime import datetime

if __name__ == "__main__":

	sc = SparkContext()

	lines = sc.textFile(sys.argv[1], 1)

	#header = lines.first()
	# Remove the header
	# lines = lines.filter(lambda x: x!=header).mapPartitions(lambda x: reader(x))
	lines = lines.mapPartitions(lambda x: reader(x))
	lines = lines.map(lambda x: (x[3])).map(lambda s: datetime.strptime(s, '%m/%d/%Y'))
	year = lines.map(lambda x: (x.year, 1)).reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[0])
	year.collect()
	year.map(lambda data: str(data[0]) + ',' + str(data[1]))
	year.saveAsTextFile("end_year_count.out")

	sc.stop()