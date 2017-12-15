from __future__ import print_function

import sys
import re
import string
from pyspark import SparkContext
from csv import reader
from operator import add

if __name__ == "__main__":
	
	# Collect the statistics
	def statistic_count(rdd):
		rdd.map(lambda row: (row, 1)) \
			.reduceByKey(lambda x, y: x + y) \
			.sortBy(lambda x: x[1], False) \
			.map(lambda row: (row[0],row[1])) \
			.saveAsTextFile("PD_CD_PARKS_NM_count.out")

	sc = SparkContext()

	lines = sc.textFile(sys.argv[1], 1)

	# header = lines.first()
	# #Remove the header
	# lines = lines.filter(lambda x: x!=header).mapPartitions(lambda x: reader(x))

	lines = lines.mapPartitions(lambda x: reader(x))

	lines = lines.map(lambda x: (x[8], x[17]))

	# Collect the statistics
	statistic_count(lines)

	sc.stop()