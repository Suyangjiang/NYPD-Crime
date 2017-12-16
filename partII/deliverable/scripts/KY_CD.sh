#!/bin/bash

/usr/bin/hadoop fs -rm -r KY_CD_count.out
spark-submit KY_CD.py /user/js9258/cleanedData.out
/usr/bin/hadoop fs -getmerge KY_CD_count.out KY_CD_count.out
