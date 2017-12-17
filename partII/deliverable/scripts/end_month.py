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

	# header = lines.first()
	# Remove the header
	lines = lines.mapPartitions(lambda x: reader(x))

	lines = lines.map(lambda x: (x[3])).map(lambda s: datetime.strptime(s, '%m/%d/%Y')).filter(lambda date: date.year >= 2005)
	year = lines.map(lambda x: (x.month, 1)).reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[0]).sortBy(lambda x: x[1], False) 
	year.collect()
	
	year.saveAsTextFile("end_month.out")

	# Collect the statistics
	#statistic_count(lines)

	sc.stop()
