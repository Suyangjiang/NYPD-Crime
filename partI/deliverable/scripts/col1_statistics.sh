/usr/bin/hadoop fs -rm -r col1_statistic_count.out
/usr/bin/hadoop fs -rm -r col1_validation_count.out
spark-submit col1.py /user/js9258/NYPD_Complaint_Data_Historic.csv
/usr/bin/hadoop fs -getmerge col1_statistic_count.out col1_statistic_count.out
/usr/bin/hadoop fs -getmerge col1_validation_count.out col1_validation_count.out