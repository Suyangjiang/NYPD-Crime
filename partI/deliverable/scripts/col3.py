from __future__ import print_function

import sys
import re
import string
import time
from operator import add
from pyspark import SparkContext
from csv import reader


if __name__ == "__main__":
	
	def baseType_time(input):
		mat=re.match('(\d{2}|\d{1}):(\d{2}|\d{1}):(\d{2}|\d{1})$', input)
		if mat is not None:
			return "DATETIME"
		else:
			return type(input)
		
	def semanticType_time(value):
		mat=re.match('(\d{2}|0?[1-9]):(\d{2}|0?[1-9]):(\d{2}|0?[1-9])$', value)
		if mat is not None:
			return "Complaint Time"
		else:
			return "Other"	
		
	def validation_time(x):
		if x == '':
			return "NULL"
		try: 
			if time.strptime(x, "%H:%M:%S"):
				mat=re.match('([01][0-9]|2[0-3]|0?[1-9]):([0-5][0-9]|0?[1-9]):([0-5][0-9]|0?[1-9])$', x)
				if mat is not None:
					return "VALID"
				else:	
					raise ValueError
		except ValueError:
			return "INVALID"

	def statistic_count(rdd, baseType_time, semanticType_time, validation_time):
		rdd.map(lambda row: (row, 1)) \
			.reduceByKey(lambda x, y: x + y) \
			.sortBy(lambda x: x[1]) \
			.map(lambda row: (row[0], baseType_time(row[0]), semanticType_time(row[0]), validation_time(row[0]), row[1])) \
			.saveAsTextFile("col3_statistic_count.out")

	sc = SparkContext()

	lines = sc.textFile(sys.argv[1], 1)
	
	header = lines.first()

	lines = lines.filter(lambda x: x!=header).mapPartitions(lambda x: reader(x))
	
	lines = lines.map(lambda x: (x[2]))

	validation_count = lines.map(lambda x: (validation_time(x), 1)) \
		.reduceByKey(lambda x, y: x + y)
						
	validation_count.saveAsTextFile("col3_validation_count.out")
	
	statistic_count(lines, baseType_time, semanticType_time, validation_time)

	sc.stop()