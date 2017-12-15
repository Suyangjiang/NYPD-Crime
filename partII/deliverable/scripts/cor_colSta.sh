#!/bin/bash

/usr/bin/hadoop fs -rm -r KY_CD_BORO_NM_count.out
spark-submit KY_CD_BORO_NM.py /user/js9258/cleaned_data.csv
/usr/bin/hadoop fs -getmerge  KY_CD_BORO_NM_count.out  KY_CD_BORO_NM_count.out

/usr/bin/hadoop fs -rm -r KY_CD_ADDR_PCT_CD_count.out
spark-submit KY_CD_ADDR_PCT_CD.py /user/js9258/cleaned_data.csv
/usr/bin/hadoop fs -getmerge KY_CD_ADDR_PCT_CD_count.out KY_CD_ADDR_PCT_CD_count.out

/usr/bin/hadoop fs -rm -r KY_CD_LOC_OF_OCCUR_DESC_count.out
spark-submit KY_CD_LOC_OF_OCCUR_DESC.py /user/js9258/cleaned_data.csv
/usr/bin/hadoop fs -getmerge KY_CD_LOC_OF_OCCUR_DESC_count.out KY_CD_LOC_OF_OCCUR_DESC_count.out

/usr/bin/hadoop fs -rm -r KY_CD_HADEVELOPT_count.out
spark-submit KY_CD_HADEVELOPT.py /user/js9258/cleaned_data.csv
/usr/bin/hadoop fs -getmerge KY_CD_HADEVELOPT_count.out KY_CD_HADEVELOPT_count.out

/usr/bin/hadoop fs -rm -r KY_CD_LAT_LON_count.out
spark-submit KY_CD_LAT_LON.py /user/js9258/cleaned_data.csv
/usr/bin/hadoop fs -getmerge KY_CD_LAT_LON_count.out KY_CD_LAT_LON_count.out

/usr/bin/hadoop fs -rm -r KY_CD_PARKS_NM_count.out
spark-submit KY_CD_PARKS_NM.py /user/js9258/cleaned_data.csv
/usr/bin/hadoop fs -getmerge KY_CD_PARKS_NM_count.out KY_CD_PARKS_NM_count.out

/usr/bin/hadoop fs -rm -r PD_CD_BORO_NM_count.out
spark-submit PD_CD_BORO_NM.py /user/js9258/cleaned_data.csv
/usr/bin/hadoop fs -getmerge PD_CD_BORO_NM_count.out PD_CD_BORO_NM_count.out

/usr/bin/hadoop fs -rm -r PD_CD_ADDR_PCT_CD_count.out
spark-submit PD_CD_ADDR_PCT_CD.py /user/js9258/cleaned_data.csv
/usr/bin/hadoop fs -getmerge PD_CD_ADDR_PCT_CD_count.out PD_CD_ADDR_PCT_CD_count.out

/usr/bin/hadoop fs -rm -r PD_CD_LOC_OF_OCCUR_DESC_count.out
spark-submit PD_CD_LOC_OF_OCCUR_DESC.py /user/js9258/cleaned_data.csv
/usr/bin/hadoop fs -getmerge PD_CD_LOC_OF_OCCUR_DESC_count.out PD_CD_LOC_OF_OCCUR_DESC_count.out

/usr/bin/hadoop fs -rm -r PD_CD_HADEVELOPT_count.out
spark-submit PD_CD_HADEVELOPT.py /user/js9258/cleaned_data.csv
/usr/bin/hadoop fs -getmerge PD_CD_HADEVELOPT_count.out PD_CD_HADEVELOPT_count.out

/usr/bin/hadoop fs -rm -r PD_CD_LAT_LON_count.out
spark-submit PD_CD_LAT_LON.py /user/js9258/cleaned_data.csv
/usr/bin/hadoop fs -getmerge PD_CD_LAT_LON_count.out PD_CD_LAT_LON_count.out

/usr/bin/hadoop fs -rm -r PD_CD_PARKS_NM_count.out
spark-submit PD_CD_PARKS_NM.py /user/js9258/cleaned_data.csv
/usr/bin/hadoop fs -getmerge PD_CD_PARKS_NM_count.out PD_CD_PARKS_NM_count.out

/usr/bin/hadoop fs -rm -r CRM_ATPT_CPTD_CD_BORO_NM_count.out
spark-submit CRM_ATPT_CPTD_CD_BORO_NM.py /user/js9258/cleaned_data.csv
/usr/bin/hadoop fs -getmerge CRM_ATPT_CPTD_CD_BORO_NM_count.out CRM_ATPT_CPTD_CD_BORO_NM_count.out

/usr/bin/hadoop fs -rm -r CRM_ATPT_CPTD_CD_ADDR_PCT_CD_count.out
spark-submit CRM_ATPT_CPTD_CD_ADDR_PCT_CD.py /user/js9258/cleaned_data.csv
/usr/bin/hadoop fs -getmerge CRM_ATPT_CPTD_CD_ADDR_PCT_CD_count.out CRM_ATPT_CPTD_CD_ADDR_PCT_CD_count.out

/usr/bin/hadoop fs -rm -r CRM_ATPT_CPTD_CD_LOC_OF_OCCUR_DESC_count.out
spark-submit CRM_ATPT_CPTD_CD_LOC_OF_OCCUR_DESC.py /user/js9258/cleaned_data.csv
/usr/bin/hadoop fs -getmerge CRM_ATPT_CPTD_CD_LOC_OF_OCCUR_DESC_count.out CRM_ATPT_CPTD_CD_LOC_OF_OCCUR_DESC_count.out

/usr/bin/hadoop fs -rm -r CRM_ATPT_CPTD_CD_HADEVELOPT_count.out
spark-submit CRM_ATPT_CPTD_CD_HADEVELOPT.py /user/js9258/cleaned_data.csv
/usr/bin/hadoop fs -getmerge CRM_ATPT_CPTD_CD_HADEVELOPT_count.out CRM_ATPT_CPTD_CD_HADEVELOPT_count.out

/usr/bin/hadoop fs -rm -r CRM_ATPT_CPTD_CD_LAT_LON_count.out
spark-submit CRM_ATPT_CPTD_CD_LAT_LON.py /user/js9258/cleaned_data.csv
/usr/bin/hadoop fs -getmerge CRM_ATPT_CPTD_CD_LAT_LON_count.out CRM_ATPT_CPTD_CD_LAT_LON_count.out

/usr/bin/hadoop fs -rm -r CRM_ATPT_CPTD_CD_PARKS_NM_count.out
spark-submit CRM_ATPT_CPTD_CD_PARKS_NM.py /user/js9258/cleaned_data.csv
/usr/bin/hadoop fs -getmerge CRM_ATPT_CPTD_CD_PARKS_NM_count.out CRM_ATPT_CPTD_CD_PARKS_NM_count.out

/usr/bin/hadoop fs -rm -r LAW_CAT_CD_BORO_NM_count.out
spark-submit LAW_CAT_CD_BORO_NM.py /user/js9258/cleaned_data.csv
/usr/bin/hadoop fs -getmerge LAW_CAT_CD_BORO_NM_count.out LAW_CAT_CD_BORO_NM_count.out

/usr/bin/hadoop fs -rm -r LAW_CAT_CD_ADDR_PCT_CD_count.out
spark-submit LAW_CAT_CD_ADDR_PCT_CD.py /user/js9258/cleaned_data.csv
/usr/bin/hadoop fs -getmerge LAW_CAT_CD_ADDR_PCT_CD_count.out LAW_CAT_CD_ADDR_PCT_CD_count.out

/usr/bin/hadoop fs -rm -r LAW_CAT_CD_LOC_OF_OCCUR_DESC_count.out
spark-submit LAW_CAT_CD_LOC_OF_OCCUR_DESC.py /user/js9258/cleaned_data.csv
/usr/bin/hadoop fs -getmerge LAW_CAT_CD_LOC_OF_OCCUR_DESC_count.out LAW_CAT_CD_LOC_OF_OCCUR_DESC_count.out

/usr/bin/hadoop fs -rm -r LAW_CAT_CD_HADEVELOPT_count.out
spark-submit LAW_CAT_CD_HADEVELOPT.py /user/js9258/cleaned_data.csv
/usr/bin/hadoop fs -getmerge LAW_CAT_CD_HADEVELOPT_count.out LAW_CAT_CD_HADEVELOPT_count.out

/usr/bin/hadoop fs -rm -r LAW_CAT_CD_LAT_LON_count.out
spark-submit LAW_CAT_CD_LAT_LON.py /user/js9258/cleaned_data.csv
/usr/bin/hadoop fs -getmerge LAW_CAT_CD_LAT_LON_count.out LAW_CAT_CD_LAT_LON_count.out

/usr/bin/hadoop fs -rm -r LAW_CAT_CD_PARKS_NM_count.out
spark-submit LAW_CAT_CD_PARKS_NM.py /user/js9258/cleaned_data.csv
/usr/bin/hadoop fs -getmerge LAW_CAT_CD_PARKS_NM_count.out LAW_CAT_CD_PARKS_NM_count.out

/usr/bin/hadoop fs -rm -r JURIS_DESC_BORO_NM_count.out
spark-submit JURIS_DESC_BORO_NM.py /user/js9258/cleaned_data.csv
/usr/bin/hadoop fs -getmerge JURIS_DESC_BORO_NM_count.out JURIS_DESC_BORO_NM_count.out

/usr/bin/hadoop fs -rm -r JURIS_DESC_ADDR_PCT_CD_count.out
spark-submit JURIS_DESC_ADDR_PCT_CD.py /user/js9258/cleaned_data.csv
/usr/bin/hadoop fs -getmerge JURIS_DESC_ADDR_PCT_CD_count.out JURIS_DESC_ADDR_PCT_CD_count.out

/usr/bin/hadoop fs -rm -r JURIS_DESC_LOC_OF_OCCUR_DESC_count.out
spark-submit JURIS_DESC_LOC_OF_OCCUR_DESC.py /user/js9258/cleaned_data.csv
/usr/bin/hadoop fs -getmerge JURIS_DESC_LOC_OF_OCCUR_DESC_count.out JURIS_DESC_LOC_OF_OCCUR_DESC_count.out

/usr/bin/hadoop fs -rm -r JURIS_DESC_HADEVELOPT_count.out
spark-submit JURIS_DESC_HADEVELOPT.py /user/js9258/cleaned_data.csv
/usr/bin/hadoop fs -getmerge JURIS_DESC_HADEVELOPT_count.out JURIS_DESC_HADEVELOPT_count.out

/usr/bin/hadoop fs -rm -r JURIS_DESC_LAT_LON_count.out
spark-submit JURIS_DESC_LAT_LON.py /user/js9258/cleaned_data.csv
/usr/bin/hadoop fs -getmerge JURIS_DESC_LAT_LON_count.out JURIS_DESC_LAT_LON_count.out

/usr/bin/hadoop fs -rm -r JURIS_DESC_PARKS_NM_count.out
spark-submit JURIS_DESC_PARKS_NM.py /user/js9258/cleaned_data.csv
/usr/bin/hadoop fs -getmerge JURIS_DESC_PARKS_NM_count.out JURIS_DESC_PARKS_NM_count.out