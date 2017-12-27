#!/usr/bin/env bash

# the Hadoop-streaming enviroment does not read the standard bash startup files,
# so we must use a wrapper to explicitly set up the modules environment and load
# the relevant modules

hadoop fs -rm -r /user/netID/YEAR_BORO_NM_count.out
hadoop fs -rm -r /user/netID/MONTH_BORO_NM_count.out
hadoop fs -rm -r /user/netID/YEAR_MONTH_BORO_NM_count.out
hadoop fs -rm -r /user/netID/YEAR_MONTH_DAY_BORO_NM_count.out
hadoop fs -rm -r /user/netID/YEAR_LOC_OF_OCCUR_DESC_count.out
hadoop fs -rm -r /user/netID/MONTH_LOC_OF_OCCUR_DESC_count.out
hadoop fs -rm -r /user/netID/YEAR_MONTH_LOC_OF_OCCUR_DESC_count.out
hadoop fs -rm -r /user/netID/YEAR_PREM_TYP_DESC_count.out
hadoop fs -rm -r /user/netID/MONTH_PREM_TYP_DESC_count.out
hadoop fs -rm -r /user/netID/YEAR_MONTH_PREM_TYP_DESC_count.out
hadoop fs -rm -r /user/netID/YEAR_PARKS_NM_count.out
hadoop fs -rm -r /user/netID/MONTH_PARKS_NM_count.out
hadoop fs -rm -r /user/netID/YEAR_MONTH_PARKS_NM_count.out
hadoop fs -rm -r /user/netID/YEAR_HADEVELOPT_count.out
hadoop fs -rm -r /user/netID/MONTH_HADEVELOPT_count.out
hadoop fs -rm -r /user/netID/YEAR_MONTH_HADEVELOPT_count.out
hadoop fs -rm -r /user/netID/YEAR_Lat_Lon_count.out
hadoop fs -rm -r /user/netID/MONTH_Lat_Lon_count.out
hadoop fs -rm -r /user/netID/YEAR_MONTH_Lat_Lon_count.out


rm -f YEAR_BORO_NM_count
rm -f MONTH_BORO_NM_count
rm -f YEAR_MONTH_BORO_NM_count
rm -f YEAR_MONTH_DAY_BORO_NM_count
rm -f YEAR_LOC_OF_OCCUR_DESC_count
rm -f MONTH_LOC_OF_OCCUR_DESC_count
rm -f YEAR_MONTH_LOC_OF_OCCUR_DESC_count
rm -f YEAR_PREM_TYP_DESC_count
rm -f MONTH_PREM_TYP_DESC_count
rm -f YEAR_MONTH_PREM_TYP_DESC_count
rm -f YEAR_PARKS_NM_count
rm -f MONTH_PARKS_NM_count
rm -f YEAR_MONTH_PARKS_NM_count
rm -f YEAR_HADEVELOPT_count
rm -f MONTH_HADEVELOPT_count
rm -f YEAR_MONTH_HADEVELOPT_count
rm -f YEAR_Lat_Lon_count
rm -f MONTH_Lat_Lon_count
rm -f YEAR_MONTH_Lat_Lon_count


spark-submit --packages com.databricks:spark-csv_2.10:1.2.0 col14-2count.py cleaned_data.csv
spark-submit --packages com.databricks:spark-csv_2.10:1.2.0 col16-2count.py cleaned_data.csv
spark-submit --packages com.databricks:spark-csv_2.10:1.2.0 col17-2count.py cleaned_data.csv
spark-submit --packages com.databricks:spark-csv_2.10:1.2.0 col18-2count.py cleaned_data.csv
spark-submit --packages com.databricks:spark-csv_2.10:1.2.0 col19-2count.py cleaned_data.csv
spark-submit --packages com.databricks:spark-csv_2.10:1.2.0 col24-2count.py cleaned_data.csv
