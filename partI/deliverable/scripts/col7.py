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
	

	def semanticType_cd(x):
		try: 
			if (len(x) == 3) and x.isdigit():
				return "Offense Key Code"
			else:
				return "Other"
		except ValueError:	
				return "Other"	
		
	def validation_key_cd(x):
		try: 
			if x == '':
				return "NULL"
			elif (len(x) == 3 and x.isdigit() and int(x) > 100 and int(x) < 900):
				return "VALID"
			else:	
				return "INVALID"
		except ValueError:
			return "INVALID"
	
	def statistic_count(rdd, baseType_int, semanticType_cd, validation_key_cd):
		rdd.map(lambda row: (row, 1)) \
			.reduceByKey(lambda x, y: x + y) \
			.sortBy(lambda x: x[1]) \
			.map(lambda row: (row[0], baseType_int(row[0]), semanticType_cd(row[0]), validation_key_cd(row[0]), row[1])) \
			.saveAsTextFile("col7_statistic_count.out")
	
	sc = SparkContext()

	lines = sc.textFile(sys.argv[1], 1)

	header = lines.first()

	lines = lines.filter(lambda x: x!=header).mapPartitions(lambda x: reader(x))
	
	lines = lines.map(lambda x: (x[6]))

	validation_count = lines.map(lambda x: (validation_key_cd(x), 1)) \
		.reduceByKey(lambda x, y: x + y)
						
	validation_count.saveAsTextFile("col7_validation_count.out")
	
	statistic_count(lines, baseType_int, semanticType_cd, validation_key_cd)
		
	
	sc.stop()