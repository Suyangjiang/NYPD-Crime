#!/bin/bash

hadoop fs -rm -r cleanedData.out
spark-submit --packages com.databricks:spark-csv_2.10:1.2.0 Data_Clean.py /user/js9258/cleaned.csv
hadoop fs -getmerge cleanedData.out cleanedData.out