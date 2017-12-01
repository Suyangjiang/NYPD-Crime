from __future__ import print_function

import sys
import re
import string
from operator import add
from pyspark import SparkContext
from csv import reader
from datetime import datetime

if __name__ == "__main__":
	def baseType_date(input):
		mat=re.match('(\d{2}|\d{1})/(\d{2}|\d{1})/(\d{4})$', input)
		if mat is not None:
			return "DATETIME"
		else:
			return type(input)
	

	def semanticType_date(value):
		mat=re.match('(\d{2}|0?[1-9])/(\d{2}|0?[1-9])/(\d{4})$', value)
		if mat is not None:
			return "Complaint Date"
		else:
			return "Other"	
		
	def validation_date(x):
		if x == '':
			return "NULL"
		try:
			if x != datetime.strptime(x, "%m/%d/%Y").strftime('%m/%d/%Y'):
				raise ValueError
			mat=re.match('(1[0-2]|0?[1-9])/(3[01]|[12][0-9]|0?[1-9])/(20)(0[6-9]|1[0-5])$', x)
			if mat is not None:
				return "VALID"
			else:	
				return "INVALID"
		except ValueError:
			return "INVALID"
			
	def statistic_count(rdd, baseType_date, semanticType_date, validation_date):
		rdd.map(lambda row: (row, 1)) \
			.reduceByKey(lambda x, y: x + y) \
			.sortBy(lambda x: x[1]) \
			.map(lambda row: (row[0], baseType_date(row[0]), semanticType_date(row[0]), validation_date(row[0]), row[1])) \
			.saveAsTextFile("col4_statistic_count.out")
	
	sc = SparkContext()

	lines = sc.textFile(sys.argv[1], 1)

	header = lines.first()

	lines = lines.filter(lambda x: x!=header).mapPartitions(lambda x: reader(x))
	
	lines = lines.map(lambda x: (x[3]))

	validation_count = lines.map(lambda x: (validation_date(x), 1)) \
		.reduceByKey(lambda x, y: x + y)
						
	validation_count.saveAsTextFile("col4_validation_count.out")
	
	statistic_count(lines, baseType_date, semanticType_date, validation_date)

	sc.stop()