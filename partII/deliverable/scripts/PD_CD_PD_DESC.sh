#!/bin/bash

/usr/bin/hadoop fs -rm -r PD_CD_PD_DESC_count.out
spark-submit PD_CD_PD_DESC.py /user/js9258/cleaned.csv
/usr/bin/hadoop fs -getmerge PD_CD_PD_DESC_count.out PD_CD_PD_DESC_count.out