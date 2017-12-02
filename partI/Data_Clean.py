from __future__ import print_function

import sys
import re
import string
import time
from pyspark import SparkContext
from csv import reader
from datetime import datetime
from operator import add

if __name__ == "__main__":

    def validation_check_number(x):
        if x == None or x == '' :
            return "NULL"
        mat=re.match('[1-9]{1}[0-9]*', x)
        if mat is not None:
            return "VALID"
        else:
            return "INVALID"

    def validation_check_date(x):
        if x == None or x == '' :
            return "NULL"
        try:
            if x != datetime.strptime(x, "%m/%d/%Y").strftime('%m/%d/%Y'):
                raise ValueError
            x = x.replace('1015', '2015')
            x = x.replace('1016', '2016')
            mat=re.match('(1[0-2]|0?[1-9])/(3[01]|[12][0-9]|0?[1-9])/(20)(0[6-9]|1[0-6])$', x)
            if mat is not None:
                return "VALID"
            else:   
                return "INVALID"
        except ValueError:
            return "INVALID"

    def validation_check_time(x):
        if x == None or x == '' :
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

    def col1_2_3_4_time_validation(x):
        try:
            from_time = datetime.strptime(x[1] + x[2], '%m/%d/%Y%H:%M:%S')
            to_time = datetime.strptime(x[3] + x[4], '%m/%d/%Y%H:%M:%S')
            if from_time < to_time:
                return "VALID"
        except ValueError:
            return "INVALID"

    def validation_check_offense_level(x):
        try: 
            if x == None or x == '' :
                return "NULL"
            elif (len(x) == 3 and x.isdigit() and int(x) > 100 and int(x) < 900):
                return "VALID"
            else:   
                return "INVALID"
        except ValueError:
            return "INVALID"

    def validation_check_crime_codes(x):
        try: 
            if x == None or x == '' :
                return "NULL"
            elif (len(x) == 3 and x.isdigit() and int(x) > 100 and int(x) < 999):
                return "VALID"
            else:   
                return "INVALID"
        except ValueError:
            return "INVALID"

    def validation_check_crime_description(x):
        x = x.upper()
        if x == None or x == '' :
            return "NULL"
        elif (len(x) > 3 and type(x) == str):
            return "VALID"
        else:   
            return "INVALID"

    def validation_check_completeness(x):
        x = x.upper()
        crimeCompleteness = ['COMPLETED', 'ATTEMPTED']
        if x == None or x == '' :
            return "NULL"
        elif x in crimeCompleteness:
            return "VALID"
        else:   
            return "INVALID"

    def validation_check_offense_seriousness(x):
        x = x.upper()
        offenseClassifications = ['MISDEMEANOR', 'VIOLATION', 'FELONY']
        if x == None or x == '' :
            return "NULL"
        elif x in offenseClassifications:
            return "VALID"
        else:   
            return "INVALID"

    def validation_check_juris(x):
        x = x.upper()
        if x == None or x == '' :
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
    
    cleanedData = lines.filter(lambda x: \
        validation_check_number(x[0]) == "VALID" and \
        validation_check_date(x[1]) == "VALID" and \
        validation_check_time(x[2]) == "VALID" and \
        validation_check_date(x[3]) != "INVALID" and \
        validation_check_time(x[4]) != "INVALID" and \
        col1_2_3_4_time_validation(x) == "VALID" and \
        validation_check_date(x[5]) == "VALID" and \
        validation_check_offense_level(x[6]) == "VALID" and \
        validation_check_crime_description(x[7]) == "VALID" and \
        validation_check_crime_codes(x[8]) == "VALID" and \
        validation_check_crime_description(x[9]) == "VALID" and \
        validation_check_completeness(x[10]) == "VALID" and \
        validation_check_offense_seriousness(x[11]) == "VALID" and \
        validation_check_juris(x[12]) == "VALID")

    cleanedData.saveAsTextFile("cleanedData.out")
    
    sc.stop()
