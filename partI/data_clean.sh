/usr/bin/hadoop fs -rm -r cleanedData.out
spark-submit Data_Clean.py /user/js9258/NYPD_Complaint_Data_Historic.csv
/usr/bin/hadoop fs -getmerge cleanedData.out cleanedData.out