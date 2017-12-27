#!/usr/bin/env bash

# the Hadoop-streaming enviroment does not read the standard bash startup files,
# so we must use a wrapper to explicitly set up the modules environment and load
# the relevant modules

hadoop fs -rm -r /user/netID/BORO_NM_count.out
hadoop fs -rm -r /user/netID/HADEVELOPT_count.out
hadoop fs -rm -r /user/netID/LOC_OF_OCCUR_DESC_count.out
hadoop fs -rm -r /user/netID/PARKS_NM_count.out
hadoop fs -rm -r /user/netID/PREM_TYP_DESC_count.out
hadoop fs -rm -r /user/netID/Lat_Lon_count.out

rm -f BORO_NM_count
rm -f HADEVELOPT_count
rm -f LOC_OF_OCCUR_DESC_count
rm -f PARKS_NM_count
rm -f PREM_TYP_DESC_count
rm -f Lat_Lon_count

spark-submit --packages com.databricks:spark-csv_2.10:1.2.0 analy_col14.py cleaned_data.csv
spark-submit --packages com.databricks:spark-csv_2.10:1.2.0 analy_col16.py cleaned_data.csv
spark-submit --packages com.databricks:spark-csv_2.10:1.2.0 analy_col17.py cleaned_data.csv
spark-submit --packages com.databricks:spark-csv_2.10:1.2.0 analy_col18.py cleaned_data.csv
spark-submit --packages com.databricks:spark-csv_2.10:1.2.0 analy_col19.py cleaned_data.csv
spark-submit --packages com.databricks:spark-csv_2.10:1.2.0 analy_col24.py cleaned_data.csv
