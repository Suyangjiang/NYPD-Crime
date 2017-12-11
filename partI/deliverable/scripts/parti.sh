#!/bin/bash

# the Hadoop-streaming enviroment does not read the standard bash startup files,
# so we must use a wrapper to explicitly set up the modules environment and load 
# the relevant modules

hadoop fs -rm -r /user/hz1076/ADDR_PCT_CD
hadoop fs -rm -r /user/hz1076/BORO_NM
hadoop fs -rm -r /user/hz1076/HADEVELOPT
hadoop fs -rm -r /user/hz1076/LOC_OF_OCCUR_DESC
hadoop fs -rm -r /user/hz1076/Latitude
hadoop fs -rm -r /user/hz1076/Longitude
hadoop fs -rm -r /user/hz1076/PARKS_NM
hadoop fs -rm -r /user/hz1076/PREM_TYP_DESC
hadoop fs -rm -r /user/hz1076/X_COORD_CD
hadoop fs -rm -r /user/hz1076/Y_COORD_CD
hadoop fs -rm -r /user/hz1076/cleaned

rm -f ADDR_PCT_CD.csv
rm -f BORO_NM.csv
rm -f HADEVELOPT.csv
rm -f Latitude.csv
rm -f LOC_OF_OCCUR_DESC.csv
rm -f Longitude.csv
rm -f PARKS_NM.csv
rm -f PREM_TYP_DESC.csv
rm -f X_COORD_CD.csv
rm -f Y_COORD_CD.csv
rm -f NYPDstatistics.txt
rm -f cleaned.csv