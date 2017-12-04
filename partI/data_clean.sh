#!/bin/bash

hadoop fs -rm -r cleanedData.out
spark-submit Data_Clean.py /user/js9258/cleaned.csv
hadoop fs -getmerge cleanedData.out cleanedData.out