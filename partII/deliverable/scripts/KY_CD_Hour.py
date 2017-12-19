from __future__ import print_function

import sys
import time
from operator import add
from pyspark import SparkContext
from csv import reader


def devidetime(x):
        KYCD = x[6].strip()
        temp = x[2].split(":")
        if temp[0] == "24":
                x[2] = ("%s:%s:%s") % (0, temp[1], temp[2])

        time_stamp = time.strptime(x[2].strip(), "%H:%M:%S")
        bound1 = time.strptime("6:00:00", "%H:%M:%S")
        bound2 = time.strptime("17:00:00", "%H:%M:%S")
        bound3 = time.strptime("23:00:00", "%H:%M:%S")
        if (time_stamp > bound1 and time_stamp <= bound2):
                x = ((KYCD, 0), 1)
        elif (time_stamp > bound2 and time_stamp <= bound3):
                x = ((KYCD, 1), 1)
        else:
                x = ((KYCD, 2), 1)
        return x

def stat(x):
        KYCD = x[0][0]
        hour = x[0][1]
        return "%s, %s, %d" % (KYCD, hour, x[1])

if __name__ == "__main__":
        sc = SparkContext()
        data = sc.textFile(sys.argv[1], 1)

        data = data.mapPartitions(lambda x: reader(x))\
                        .map(devidetime)\
                        .reduceByKey(add)\
                        .filter(lambda x: x[0])\
                        .map(stat)
        data.saveAsTextFile("KYCD_hour_amount.out")

        sc.stop()