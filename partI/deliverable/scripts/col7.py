from __future__ import print_function

import sys
import re
import string
from pyspark import SparkContext
from csv import reader
from operator import add

if __name__ == "__main__":
	
	def baseType_check(input):
		try:
			number = int(input)
			return "INT"
		except ValueError:
			return type(input)
	

	def semanticType_check(value):
		try: 
			if (len(value) == 3) and value.isdigit():
				return "Offense Level"
			else:
				return "Other"
		except ValueError:	
				return "Other"	
		
	def validation_check(x):
		try: 
			if x == '':
				return "NULL"
			elif (len(x) == 3 and x.isdigit() and int(x) > 100 and int(x) < 900):
				return "VALID"
			else:	
				return "INVALID"
		except ValueError:
			return "INVALID"

	# Collect the statistics
	def statistic_count(rdd, baseType_check, semanticType_check, validation_check):
		rdd.map(lambda row: (row, 1)) \
			.reduceByKey(lambda x, y: x + y) \
			.sortBy(lambda x: x[1]) \
			.map(lambda row: (row[0], baseType_check(row[0]), semanticType_check(row[0]), validation_check(row[0]), row[1])) \
			.saveAsTextFile("col7_statistic_count.out")


	sc = SparkContext()

	lines = sc.textFile(sys.argv[1], 1)

	header = lines.first()
	# Remove the header
	lines = lines.filter(lambda x: x!=header).mapPartitions(lambda x: reader(x))

	lines = lines.map(lambda x: (x[6]))
	# Statistic VALID, INVALID, and NULL values 
	validation_count = lines.map(lambda x: (validation_check(x), 1)) \
		.reduceByKey(lambda x, y: x + y)

	validation_count.saveAsTextFile("col7_validation_count.out")
	# Collect the statistics
	statistic_count(lines, baseType_check, semanticType_check, validation_check)

	sc.stop()