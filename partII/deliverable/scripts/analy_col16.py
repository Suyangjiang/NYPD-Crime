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


# Collect the statistics
def statistic_count(rdd):
    rdd.map(lambda row: (row, 1)) \
        .reduceByKey(lambda x, y: x + y) \
        .sortBy(lambda x: x[1], False) \
        .map(lambda row: (row[0],row[1])) \
        .saveAsTextFile("LOC_OF_OCCUR_DESC_count.out")


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: spark-submit --packages com.databricks:spark-csv_2.10:1.2.0 analy_col16.py <input>")
        exit(-1)
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)

    header = lines.first()
    # Remove the header
    lines = lines.filter(lambda x: x != header).mapPartitions(lambda x: reader(x))

    lines = lines.map(lambda x: (x[15]))

    # Collect the statistics
    statistic_count(lines)

    command = 'hadoop fs -getmerge /user/netID/LOC_OF_OCCUR_DESC_count.out LOC_OF_OCCUR_DESC_count'
    os.system(command)

    sc.stop()
