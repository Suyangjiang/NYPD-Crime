from __future__ import print_function

import sys
import re
import string
import time
from pyspark import SparkContext
from csv import reader
from datetime import datetime
from operator import add

if __name__ == "__main__":

    def validation_check_number(x):
        if x == None or x == '' :
            return "NULL"
        mat=re.match('[1-9]{1}[0-9]*', x)
        if mat is not None:
            return "VALID"
        else:
            return "INVALID"

    def validation_check_date(x):
        if x == None or x == '' :
            return "NULL"
        try:
            if x != datetime.strptime(x, "%m/%d/%Y").strftime('%m/%d/%Y'):
                raise ValueError
            x = x.replace('1015', '2015')
            x = x.replace('1016', '2016')
            mat=re.match('(1[0-2]|0?[1-9])/(3[01]|[12][0-9]|0?[1-9])/(20)(0[6-9]|1[0-6])$', x)
            if mat is not None:
                return "VALID"
            else:   
                return "INVALID"
        except ValueError:
            return "INVALID"

    def validation_check_time(x):
        if x == None or x == '' :
            return "NULL"
        try: 
            if time.strptime(x, "%H:%M:%S"):
                mat=re.match('([01][0-9]|2[0-3]|0?[1-9]):([0-5][0-9]|0?[1-9]):([0-5][0-9]|0?[1-9])$', x)
                if mat is not None:
                    return "VALID"
                else:   
                    raise ValueError
        except ValueError:
            return "INVALID"

    def col1_2_3_4_time_validation(x):
        try:
            from_time = datetime.strptime(x[1] + x[2], '%m/%d/%Y%H:%M:%S')
            to_time = datetime.strptime(x[3] + x[4], '%m/%d/%Y%H:%M:%S')
            if from_time < to_time:
                return "VALID"
        except ValueError:
            return "INVALID"

    def validation_check_offense_level(x):
        try: 
            if x == None or x == '' :
                return "NULL"
            elif (len(x) == 3 and x.isdigit() and int(x) > 100 and int(x) < 900):
                return "VALID"
            else:   
                return "INVALID"
        except ValueError:
            return "INVALID"

    def validation_check_crime_codes(x):
        try: 
            if x == None or x == '' :
                return "NULL"
            elif (len(x) == 3 and x.isdigit() and int(x) > 100 and int(x) < 999):
                return "VALID"
            else:   
                return "INVALID"
        except ValueError:
            return "INVALID"

    def validation_check_crime_description(x):
        x = x.upper()
        if x == None or x == '' :
            return "NULL"
        elif (len(x) > 3 and type(x) == str):
            return "VALID"
        else:   
            return "INVALID"

    def validation_check_completeness(x):
        x = x.upper()
        crimeCompleteness = ['COMPLETED', 'ATTEMPTED']
        if x == None or x == '' :
            return "NULL"
        elif x in crimeCompleteness:
            return "VALID"
        else:   
            return "INVALID"

    def validation_check_offense_seriousness(x):
        x = x.upper()
        offenseClassifications = ['MISDEMEANOR', 'VIOLATION', 'FELONY']
        if x == None or x == '' :
            return "NULL"
        elif x in offenseClassifications:
            return "VALID"
        else:   
            return "INVALID"

    def validation_check_juris(x):
        x = x.upper()
        if x == None or x == '' :
            return "NULL"
        elif (len(x) > 4 and type(x) == str):
            return "VALID"
        else:   
            return "INVALID"

    dic_6_7 = {'101':'MURDER & NON-NEGL. MANSLAUGHTER', '102':'HOMICIDE-NEGLIGENT-VEHICLE', '103':'HOMICIDE-NEGLIGENT,UNCLASSIFIE', '104':'RAPE', '105':'ROBBERY', '106':'FELONY ASSAULT', '107':'BURGLARY','109':'GRAND LARCENY','110':'GRAND LARCENY OF MOTOR VEHICLE','111':'POSSESSION OF STOLEN PROPERTY','112':'THEFT-FRAUD','113':'FORGERY','114':'ARSON','115':'PROSTITUTION & RELATED OFFENSES','116':'SEX CRIMES','117':'DANGEROUS DRUGS','118':'DANGEROUS WEAPONS','119':'INTOXICATED/IMPAIRED DRIVING','120':'CHILD ABANDONMENT/NON SUPPORT','121':'CRIMINAL MISCHIEF & RELATED OF','122':'GAMBLING','123':'ABORTION','124':'KIDNAPPING & RELATED OFFENSES','125':'NYS LAWS-UNCLASSIFIED FELONY','126':'MISCELLANEOUS PENAL LAW','230':'JOSTLING','231':"BURGLAR'S TOOLS",'232':'POSSESSION OF STOLEN PROPERTY','233':'SEX CRIMES','234':'PROSTITUTION & RELATED OFFENSES','235':'DANGEROUS DRUGS','236':'DANGEROUS WEAPONS','237':'ESCAPE 3','238':'FRAUDULENT ACCOSTING','340':'FRAUDS','341':'PETIT LARCENY','342':'PETIT LARCENY OF MOTOR VEHICLE','343':'OTHER OFFENSES RELATED TO THEF','344':'ASSAULT 3 & RELATED OFFENSES','345':'OFFENSES RELATED TO CHILDREN','346':'ALCOHOLIC BEVERAGE CONTROL LAW','347':'INTOXICATED & IMPAIRED DRIVING','348':'VEHICLE AND TRAFFIC LAWS','349':'DISRUPTION OF A RELIGIOUS SERV','350':'GAMBLING','351':'CRIMINAL MISCHIEF & RELATED OF','352':'CRIMINAL TRESPASS','353':'UNAUTHORIZED USE OF A VEHICLE','354':'ANTICIPATORY OFFENSES','355':'OFFENSES AGAINST THE PERSON','356':'PROSTITUTION & RELATED OFFENSES','357':'FORTUNE TELLING','358':'OFFENSES INVOLVING FRAUD','359':'OFFENSES AGAINST PUBLIC ADMINI','360':'LOITERING FOR DRUG PURPOSES','361':'OFF. AGNST PUB ORD SENSBLTY &','362':'OFFENSES AGAINST MARRIAGE UNCL','363':'OFFENSES AGAINST PUBLIC SAFETY','364':'OTHER STATE LAWS (NON PENAL LAW)','365':'ADMINISTRATIVE CODE','366':'NEW YORK CITY HEALTH CODE','455':'UNLAWFUL POSS. WEAP. ON SCHOOL','460':'LOITERING/DEVIATE SEX','571':'LOITERING/GAMBLING (CARDS, DIC','572':'DISORDERLY CONDUCT','577':'UNDER THE INFLUENCE OF DRUGS','578':'HARRASSMENT 2','672':'LOITERING','675':'ADMINISTRATIVE CODE','676':'NEW YORK CITY HEALTH CODE','677':'OTHER STATE LAWS','678':'MISCELLANEOUS PENAL LAW','685':'ADMINISTRATIVE CODES','881':'OTHER TRAFFIC INFRACTION'}
    
    sc = SparkContext()

    lines = sc.textFile(sys.argv[1], 1)

    header = lines.first()
    
    # Remove the header
    lines = lines.filter(lambda x: x!=header).mapPartitions(lambda x: reader(x))

    # fill the col_7 : description
    lines = lines.map(lambda x: (x[0],x[1],x[2],x[3],x[4],x[5],x[6],dic_6_7[x[6]],x[8],x[9],x[10],x[11],x[12],x[13],x[14],x[15],x[16],x[17],x[18],x[19],x[20],x[21],x[22],x[23]))
            
    
    cleanedData = lines.filter(lambda x: \
        validation_check_number(x[0]) == "VALID" and \
        validation_check_date(x[1]) == "VALID" and \
        validation_check_time(x[2]) == "VALID" and \
        validation_check_date(x[3]) != "INVALID" and \
        validation_check_time(x[4]) != "INVALID" and \
        col1_2_3_4_time_validation(x) == "VALID" and \
        validation_check_date(x[5]) == "VALID" and \
        validation_check_offense_level(x[6]) == "VALID" and \
        validation_check_crime_codes(x[8]) == "VALID" and \
        validation_check_crime_description(x[9]) == "VALID" and \
        validation_check_completeness(x[10]) == "VALID" and \
        validation_check_offense_seriousness(x[11]) == "VALID" and \
        validation_check_juris(x[12]) == "VALID")

    cleanedData.saveAsTextFile("cleanedData.out")
    
    sc.stop()
