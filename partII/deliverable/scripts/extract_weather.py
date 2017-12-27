#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function

import sys, os
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import col, when, udf
from pyspark.sql.types import BooleanType
from pyspark.sql import Row
from csv import reader
from datetime import datetime


# Collect the statistics
def statistic_count_year_month_weather(rdd):
    rdd.map(lambda row: (row, 1)) \
        .reduceByKey(lambda x, y: x + y) \
        .sortBy(lambda x: x[0], False) \
        .map(lambda row: (row[0], row[1])) \
        .saveAsTextFile("YEAR_MONTH_weather_count.out")



if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: spark-submit --packages com.databricks:spark-csv_2.10:1.2.0 col14-12count.py <input>")
        exit(-1)
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)

    header = lines.first()
    # Remove the header
    lines = lines.filter(lambda x: x != header).mapPartitions(lambda x: reader(x))

    lines1 = lines.map(lambda x: (x[2], x[3])).map(lambda x: (x[0].strip(' '), x[1].strip(' '))).map(lambda x: (x[0][0:6], x[1]))
    lines1 = lines1.map(lambda x: (x[0][-2:] + '/' + x[0][0:4], float(x[1])))

    sumCount = lines1.combineByKey(lambda value: (value, 1), lambda x, value: (x[0] + value, x[1] + 1), lambda x, y: (x[0] + y[0], x[1] + y[1]))
    averageByKey = sumCount.map(lambda (label, (value_sum, count)): (label, value_sum / count))


    # Collect the statistics
    statistic_count_year_month_weather(averageByKey)

    command = 'hadoop fs -getmerge /user/netID/YEAR_MONTH_weather_count.out YEAR_MONTH_weather_count'
    os.system(command)

    sc.stop()
