#!/bin/bash

/usr/bin/hadoop fs -rm -r KY_CD_OFNS_DESC_count.out
spark-submit KY_CD_OFNS_DESC.py /user/js9258/cleaned_data.csv
/usr/bin/hadoop fs -getmerge KY_CD_OFNS_DESC_count.out KY_CD_OFNS_DESC_count.out

/usr/bin/hadoop fs -rm -r PD_CD_PD_DESC_count.out
spark-submit PD_CD_PD_DESC.py /user/js9258/cleaned_data.csv
/usr/bin/hadoop fs -getmerge PD_CD_PD_DESC_count.out PD_CD_PD_DESC_count.out

/usr/bin/hadoop fs -rm -r CRM_ATPT_CPTD_CD_count.out
spark-submit CRM_ATPT_CPTD_CD.py /user/js9258/cleaned_data.csv
/usr/bin/hadoop fs -getmerge CRM_ATPT_CPTD_CD_count.out CRM_ATPT_CPTD_CD_count.out

/usr/bin/hadoop fs -rm -r LAW_CAT_CD_count.out
spark-submit LAW_CAT_CD.py /user/js9258/cleaned_data.csv
/usr/bin/hadoop fs -getmerge LAW_CAT_CD_count.out LAW_CAT_CD_count.out

/usr/bin/hadoop fs -rm -r JURIS_DESC_count.out
spark-submit JURIS_DESC.py /user/js9258/cleaned_data.csv
/usr/bin/hadoop fs -getmerge JURIS_DESC_count.out JURIS_DESC_count.out