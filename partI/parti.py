#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

def toCSVHelper(data):
    return ','.join(str(d) for d in data)

# function takes in dataframe and column name and return the counts of each distinct value in the column
def showCount(df, col, output):
    dfc = df.groupBy(col).count()
    rdd = dfc.map(lambda x: x)
    rdd1 = rdd.map(lambda x: (x[0], x[1]))
    lines = rdd1.map(toCSVHelper)
    # save on hdfs
    lines.saveAsTextFile(col)
    # save on dumbo
    command = "hadoop fs -getmerge " + output + '/' + col + ' ' + col + '.csv'
    os.system(command)

# function specific for XY coord
def showCountForXY(df, col, output):
    dfc = df.groupBy(col).count()
    rdd = dfc.map(lambda x: x)
    rdd1 = rdd.map(lambda x: (x[0], x[1]))
    rdd2 = rdd1.map(lambda x: (int(x[0].replace(',', '')) if x[0] != '' else '', x[1]))
    lines = rdd2.map(toCSVHelper)
    # save on hdfs
    lines.saveAsTextFile(col)
    # save on dumbo
    command = "hadoop fs -getmerge " + output + '/' + col + ' ' + col + '.csv'
    os.system(command)


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
    showCount(df, 'BORO_NM', output)
    list.append('BORO_NM')
    # ADDR_PCT_CD
    statistics(df, 'ADDR_PCT_CD', f)
    showCount(df, 'ADDR_PCT_CD', output)
    list.append('ADDR_PCT_CD')
    # LOC_OF_OCCUR_DESC
    statistics(df, 'LOC_OF_OCCUR_DESC', f)
    showCount(df, 'LOC_OF_OCCUR_DESC', output)
    list.append('LOC_OF_OCCUR_DESC')
    # PREM_TYP_DESC
    statistics(df, 'PREM_TYP_DESC', f)
    showCount(df, 'PREM_TYP_DESC', output)
    list.append('PREM_TYP_DESC')
    # PARKS_NM
    statistics(df, 'PARKS_NM', f)
    showCount(df, 'PARKS_NM', output)
    list.append('PARKS_NM')
    # HADEVELOPT
    statistics(df, 'HADEVELOPT', f)
    showCount(df, 'HADEVELOPT', output)
    list.append('HADEVELOPT')
    # X_COORD_CD
    statistics(df, 'X_COORD_CD', f)
    showCountForXY(df, 'X_COORD_CD', output)
    list.append('X_COORD_CD')
    # Y_COORD_CD
    statistics(df, 'Y_COORD_CD', f)
    showCountForXY(df, 'Y_COORD_CD', output)
    list.append('Y_COORD_CD')
    # Latitude
    statistics(df, 'Latitude', f)
    showCount(df, 'Latitude', output)
    list.append('Latitude')
    # Longitude
    statistics(df, 'Longitude', f)
    showCount(df, 'Longitude', output)
    list.append('Longitude')
    f.close()
    df = clean(df, list)
    df.write.format('com.databricks.spark.csv').option("header", "true").save('cleaned')
    command = "hadoop fs -getmerge " + output + "/cleaned cleaned.csv"
    os.system(command)
    sc.stop()