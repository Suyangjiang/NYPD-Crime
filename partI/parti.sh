#!/bin/bash

# the Hadoop-streaming enviroment does not read the standard bash startup files,
# so we must use a wrapper to explicitly set up the modules environment and load 
# the relevant modules

hadoop fs -rm -r /user/yournetID/ADDR_PCT_CD
hadoop fs -rm -r /user/yournetID/BORO_NM
hadoop fs -rm -r /user/yournetID/HADEVELOPT
hadoop fs -rm -r /user/yournetID/LOC_OF_OCCUR_DESC
hadoop fs -rm -r /user/yournetID/Latitude
hadoop fs -rm -r /user/yournetID/Longitude
hadoop fs -rm -r /user/yournetID/PARKS_NM
hadoop fs -rm -r /user/yournetID/PREM_TYP_DESC
hadoop fs -rm -r /user/yournetID/X_COORD_CD
hadoop fs -rm -r /user/yournetID/Y_COORD_CD
hadoop fs -rm -r /user/yournetID/cleaned

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