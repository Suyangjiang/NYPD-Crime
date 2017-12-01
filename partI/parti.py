#!/usr/bin/env python
from __future__ import print_function

import sys, os
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import col

# function takes in dataframe and column name and return the dataframe where the column value is not null
def checkNull(df, x):
    return df.where(col(x).isNull())

# function takes in dataframe and column name and return the dataframe where the column value is not empty
def checkBlank(df, x):
    return df.where(col(x) == "")

def checkValid(df, x):
    df = df.where(col(x) != "")
    df = df.where(col(x).isNotNull())
    return df

def clean(df, list):
    for enum in list:
        df = df.where(col(enum) != "")
        df = df.where(col(enum).isNotNull())
    return df

# function takes in dataframe and column name and return the counts of each distinct value in the column
def showCount(df, col, file):
    dfc = df.groupBy(col).count()
    file.write('column is: ' + col + '\n')

# get stats
def statistics(df, col, file):
    file.write('column is: ' + col + '\n')
    # statistics about null value
    dfn = checkNull(df, col)
    line = 'null: ' + str(dfn.count()) + '\n'
    file.write(line)
    # statistics about blank value
    dfb = checkBlank(df, col)
    line = 'blank: ' + str(dfb.count()) + '\n'
    file.write(line)
    # statistics about valid value
    dfv = checkValid(df, col)
    line = 'valid: ' + str(dfv.count()) + '\n'
    file.write(line)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: spark-submit --packages com.databricks:spark-csv_2.10:1.2.0 parti.py <input> <output>")
        exit(-1)
    sc = SparkContext()
    lines = sys.argv[1]
    output = sys.argv[2]
    f = open('NYPDstatistics.txt', 'a')
    sqlContext = SQLContext(sc)
    df = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load(lines)
    list = []
    # get stats of BORO_NM
    statistics(df, 'BORO_NM', f)
    list.append('BORO_NM')
    # ADDR_PCT_CD
    statistics(df, 'ADDR_PCT_CD', f)
    list.append('ADDR_PCT_CD')
    # LOC_OF_OCCUR_DESC
    statistics(df, 'LOC_OF_OCCUR_DESC', f)
    list.append('LOC_OF_OCCUR_DESC')
    # PREM_TYP_DESC
    statistics(df, 'PREM_TYP_DESC', f)
    list.append('PREM_TYP_DESC')
    # PARKS_NM
    statistics(df, 'PARKS_NM', f)
    list.append('PARKS_NM')
    # HADEVELOPT
    statistics(df, 'HADEVELOPT', f)
    list.append('HADEVELOPT')
    # X_COORD_CD
    statistics(df, 'X_COORD_CD', f)
    list.append('X_COORD_CD')
    # Y_COORD_CD
    statistics(df, 'Y_COORD_CD', f)
    list.append('Y_COORD_CD')
    # Latitude
    statistics(df, 'Latitude', f)
    list.append('Latitude')
    # Longitude
    statistics(df, 'Longitude', f)
    list.append('Longitude')
    f.close()
    df = clean(df, list)
    df.write.format('com.databricks.spark.csv').option("header", "true").save('cleaned')
    command = "hadoop fs -getmerge " + output + "/cleaned cleaned.csv"
    os.system(command)
    sc.stop()
