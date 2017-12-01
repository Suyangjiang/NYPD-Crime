from __future__ import print_function

import sys
import re
import string
from pyspark import SparkContext
from csv import reader
from datetime import datetime
from operator import add

if __name__ == "__main__":

	def validation_check_offense_level(x):
		try: 
			if x == '':
				return "NULL"
			elif (len(x) == 3 and x.isdigit() and int(x) > 100 and int(x) < 900):
				return "VALID"
			else:	
				return "INVALID"
		except ValueError:
			return "INVALID"

	def validation_check_crime_codes(x):
		try: 
			if x == '':
				return "NULL"
			elif (len(x) == 3 and x.isdigit() and int(x) > 100 and int(x) < 999):
				return "VALID"
			else:	
				return "INVALID"
		except ValueError:
			return "INVALID"

	def validation_check_crime_description(x):
		x = x.upper()
		if x == '':
			return "NULL"
		elif (len(x) > 3 and type(x) == str):
			return "VALID"
		else:	
			return "INVALID"

	def validation_check_completeness(x):
		x = x.upper()
		crimeCompleteness = ['COMPLETED', 'ATTEMPTED']
		if x == '':
			return "NULL"
		elif x in crimeCompleteness:
			return "VALID"
		else:	
			return "INVALID"

	def validation_check_offense_seriousness(x):
		x = x.upper()
		offenseClassifications = ['MISDEMEANOR', 'VIOLATION', 'FELONY']
		if x == '':
			return "NULL"
		elif x in offenseClassifications:
			return "VALID"
		else:	
			return "INVALID"

	def validation_check_juris(x):
		x = x.upper()
		if x == '':
			return "NULL"
		elif (len(x) > 4 and type(x) == str):
			return "VALID"
		else:	
			return "INVALID"	

	sc = SparkContext()

	lines = sc.textFile(sys.argv[1], 1)

	header = lines.first()
	
	# Remove the header
	lines = lines.filter(lambda x: x!=header).mapPartitions(lambda x: reader(x))			
	
	cleanedData = lines.filter(lambda x: validation_check_offense_level(x[6]) == "VALID" and validation_check_crime_description(x[7]) == "VALID" and \
			validation_check_crime_codes(x[8]) == "VALID" and \
			validation_check_crime_description(x[9]) == "VALID" and \
			validation_check_completeness(x[10]) == "VALID" and \
			validation_check_offense_seriousness(x[11]) == "VALID" and \
			validation_check_juris(x[12]) == "VALID")

	
	cleanedData.saveAsTextFile("cleanedData.out")
	
	sc.stop()
