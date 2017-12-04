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


def mk_int(s):
    s = s.strip()
    return int(s) if s else 0


def clean(df):
    d = {}
    with open("BORO_ADDR_key_value.txt") as f:
        for line in f:
            (key, val) = line.split(" ", 1)
            d[int(key)] = val
    # dfNew = df.withColumn("BORO_NM", replace("BORO_NM"))
    rdd = df.map(lambda x: (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10], x[11], x[12],
                            x[13] if not d.has_key(mk_int(x[14])) or x[13] == d[mk_int(x[14])] else d[mk_int(x[14])],
                            x[14], x[15], x[16], x[17], x[18], x[19], x[20], x[21], x[22], x[23]))
    df_withcol = rdd.toDF(['CMPLNT_NUM','CMPLNT_FR_DT','CMPLNT_FR_TM','CMPLNT_TO_DT','CMPLNT_TO_TM','RPT_DT','KY_CD',
                           'OFNS_DESC','PD_CD','PD_DESC','CRM_ATPT_CPTD_CD','LAW_CAT_CD','JURIS_DESC','BORO_NM',
                           'ADDR_PCT_CD','LOC_OF_OCCUR_DESC','PREM_TYP_DESC','PARKS_NM','HADEVELOPT','X_COORD_CD',
                           'Y_COORD_CD','Latitude','Longitude','Lat_Lon'])
    return df_withcol


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
    f.close()
    # clean BORO_NM
    df = clean(df)
    df.write.format('com.databricks.spark.csv').option("header", "true").save('cleaned')
    command = "hadoop fs -getmerge " + output + "/cleaned cleaned.csv"
    os.system(command)
    sc.stop()