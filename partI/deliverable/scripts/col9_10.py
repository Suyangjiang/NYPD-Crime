from __future__ import print_function

import sys
import re
import string
from pyspark import SparkContext
from csv import reader
from operator import add

if __name__ == "__main__":
	
	sc = SparkContext()

	lines = sc.textFile(sys.argv[1], 1)
	# Remove the header
	header = lines.first()
	# Statistic VALID, INVALID, and NULL values 
	lines = lines.filter(lambda x: x!=header).mapPartitions(lambda x: reader(x))

	lines = lines.map(lambda x: (x[8], x[9]))

	validation_count = lines.map(lambda x: (x, 1)) \
		.reduceByKey(lambda x, y: x + y) \
		.sortBy(lambda x: x[0])

	validation_count.saveAsTextFile("col9_10_statistics.out")

	sc.stop()