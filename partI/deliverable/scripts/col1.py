from __future__ import print_function

import sys
import re
import string
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
	
	def baseType_int(input):
		try:
			number = int(input)
			return "INT"
		except ValueError:
			return type(input)
	

	def semanticType_number(value):
		mat=re.match('[1-9]{1}[0-9]*', value)
		if mat is not None:
			return "Complaint Number"
		else:
			return "Other"	
		
	def validation_number(x):
		if x == '':
			return "NULL"
		mat=re.match('[1-9]{1}[0-9]*', x)
		if mat is not None:
			return "VALID"
		else:
			return "INVALID"
	
	def statistic_count(rdd, baseType_int, semanticType_number, validation_number):
		rdd.map(lambda row: (row, 1)) \
			.reduceByKey(lambda x, y: x + y) \
			.sortBy(lambda x: x[1]) \
			.map(lambda row: (row[0], baseType_int(row[0]), semanticType_number(row[0]), validation_number(row[0]), row[1])) \
			.saveAsTextFile("col1_statistic_count.out")

	sc = SparkContext()

	lines = sc.textFile(sys.argv[1], 1)

	header = lines.first()

	lines = lines.filter(lambda x: x!=header).mapPartitions(lambda x: reader(x))

	lines = lines.map(lambda x: (x[0]))

	validation_count = lines.map(lambda x: (validation_number(x), 1)) \
		.reduceByKey(lambda x, y: x + y)

	validation_count.saveAsTextFile("col1_validation_count.out")
	
	statistic_count(lines, baseType_int, semanticType_number, validation_number)

	sc.stop()