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
def statistic_count_year_boro(rdd):
    rdd.map(lambda row: (row, 1)) \
        .reduceByKey(lambda x, y: x + y) \
        .sortBy(lambda x: x[1], False) \
        .map(lambda row: (row[0],row[1])) \
        .saveAsTextFile("YEAR_BORO_NM_count.out")


def statistic_count_month_boro(rdd):
    rdd.map(lambda row: (row, 1)) \
        .reduceByKey(lambda x, y: x + y) \
        .sortBy(lambda x: x[1], False) \
        .map(lambda row: (row[0], row[1])) \
        .saveAsTextFile("MONTH_BORO_NM_count.out")


def statistic_count_year_month_boro(rdd):
    rdd.map(lambda row: (row, 1)) \
        .reduceByKey(lambda x, y: x + y) \
        .sortBy(lambda x: x[0], False) \
        .map(lambda row: (row[0], row[1])) \
        .saveAsTextFile("YEAR_MONTH_BORO_NM_count.out")


def statistic_count_year_month_day_boro(rdd):
    rdd.map(lambda row: (row, 1)) \
        .reduceByKey(lambda x, y: x + y) \
        .sortBy(lambda x: x[0], False) \
        .map(lambda row: (row[0], row[1])) \
        .saveAsTextFile("YEAR_MONTH_DAY_BORO_NM_count.out")


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: spark-submit --packages com.databricks:spark-csv_2.10:1.2.0 col14-12count.py <input>")
        exit(-1)
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)

    header = lines.first()
    # Remove the header
    lines = lines.filter(lambda x: x != header).mapPartitions(lambda x: reader(x))

    lines1 = lines.map(lambda x: (x[1], x[13])).map(lambda s: (datetime.strptime(s[0], '%m/%d/%Y'), s[1])).filter(lambda x: x[0].year >= 2005)
    lines2 = lines.map(lambda x: (x[1], x[13])).filter(lambda x: int(x[0][-4:]) >= 2005)

    year = lines1.map(lambda x: (x[0].year, x[1]))
    month = lines1.map(lambda x: (x[0].month, x[1]))
    year_month = lines2.map(lambda x: (x[0][0:3] + x[0][-4:], x[1]))
    year_month_day = lines2.map(lambda x: (x[0], x[1]))


    # Collect the statistics
    statistic_count_year_boro(year)
    statistic_count_month_boro(month)
    statistic_count_year_month_boro(year_month)
    statistic_count_year_month_day_boro(year_month_day)


    command = 'hadoop fs -getmerge /user/netID/YEAR_BORO_NM_count.out YEAR_BORO_NM_count'
    os.system(command)
    command = 'hadoop fs -getmerge /user/netID/MONTH_BORO_NM_count.out MONTH_BORO_NM_count'
    os.system(command)
    command = 'hadoop fs -getmerge /user/netID/YEAR_MONTH_BORO_NM_count.out YEAR_MONTH_BORO_NM_count'
    os.system(command)
    command = 'hadoop fs -getmerge /user/netID/YEAR_MONTH_DAY_BORO_NM_count.out YEAR_MONTH_DAY_BORO_NM_count'
    os.system(command)

    sc.stop()
