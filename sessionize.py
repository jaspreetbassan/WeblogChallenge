from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf
import re

#configure spark

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

sc = spark.sparkContext


#load the weblog textfile, create an RDD by splittint the data, initialize the header names, and prepare the dataframe for using SQL


weblog = sc.textFile("data/2015_07_22_mktplace_shop_web_log_sample.log")                                                                        

parts = weblog.map(lambda l: re.findall(r'(?:[^\s,"]|"(?:\\.|[^"])*")+', l)) #findall handles empty matches better than other known functions

weblogdata = parts.map(lambda p: Row(timestamp=p[0], elb=p[1], clientip=p[2], backendip=p[3], requestprocessingtime=float(p[4]), backendprocessingtime=float(p[5]), responseprocessingtime=float(p[6]), elbstatuscode=int(p[7]), backendstatuscode=int(p[8]), receivedbytes=int(p[9]), sentbytes=int(p[10]), request=p[11], useragent=p[12], sslcipher=p[13], sslprotocol=p[14]))


schemaWebLogData = spark.createDataFrame(weblogdata)
schemaWebLogData.createOrReplaceTempView("weblogdata")


#PART1**************************************************************************************************************************************



def CHARINDEX(chr, string):  #helper function to help extract only the IP part of the client_ip_port, i.e, extract the index of ':' 
	for i in range(0,len(string)):
		if(string[i]==chr):
			return i

spark.udf.register("CHARINDEX", CHARINDEX)



#Sessionize the weblog by IP with the time window set to 15. The dense_rank is created by considering the IP, useragent, date:timestamp, hours:timestamp, and minutes:timestamp as one big and sole partition where the minute value is divided by 15 for aggregating minutes under the 15 minutes group. 
session = spark.sql("select dense_rank() over (order by SUBSTR(clientip, 0, CHARINDEX(':', clientip)), useragent, substr(timestamp,1,10), substr(timestamp,12,2), round(int(substr(timestamp,15,2))/15)) as sessionID, * from weblogdata order by clientip, timestamp")


session.createOrReplaceTempView("session")
totalsessions = spark.sql("select count(distinct sessionID) as unique_sessions from session")
totalsessions.show() #display total unique sessions



#PART2**************************************************************************************************************************************



#Helper function to calculate the total time taken given two strings of the format "HH:MM:SS." Maximum time is passed to str1 and minimum to str2
def TIMETAKEN(str1, str2): 
	endtime = str1.split(":")
	starttime = str2.split(":")
	hourdelta = int(endtime[0]) - int(starttime[0])
	mindelta = int(endtime[1]) - int(starttime[1])
	secdelta = int(endtime[2]) - int(starttime[2])
	if secdelta < 0 and mindelta < 0:
		mindelta = mindelta + 1	# since the goal here is to subtract and mindelta is already negative, 1 is added
		return str(hourdelta-1).zfill(2) + ":" + str(mindelta+60).zfill(2) + ":" + str(secdelta+60).zfill(2)
	elif secdelta < 0:
		return str(hourdelta).zfill(2) + ":" + str(mindelta-1).zfill(2) + ":" + str(secdelta+60).zfill(2)
	elif mindelta < 0:
		return str(hourdelta-1).zfill(2) + ":" + str(mindelta+60).zfill(2) + ":" + str(secdelta).zfill(2)
	else:
		return str(hourdelta).zfill(2) + ":" + str(mindelta).zfill(2) + ":" + str(secdelta).zfill(2)

spark.udf.register("TIMETAKEN", TIMETAKEN)



#Determine the average session time by first calculating the time of each session
sessiontime = spark.sql("select sessionID, TIMETAKEN(max(substr(timestamp,12,8)),min(substr(timestamp,12,8))) time from session group by sessionID order by time DESC")
sessiontime.createOrReplaceTempView("sessiontime")

#Considering that every session is a time window of 15 minutes, an average of hours, minutes, and seconds was taken and returned as one string
avg_session_time = spark.sql("select concat(int(avg(int(substr(time,1,2)))), ':', int(avg(int(substr(time,4,2)))), ':', int(avg(int(substr(time,7,2))))) as avg_session_time from sessiontime") 
avg_session_time.show()




#PART3**************************************************************************************************************************************



#Helper function to extract the url part of the request field.
def EXTRACTURL(string): 
	url = string.split(" ")
	return url[1]


spark.udf.register("EXTRACTURL", EXTRACTURL)

#Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session
uniqueurl = spark.sql("select sessionID, count(distinct EXTRACTURL(request)) as unique_urls from session group by sessionID")
uniqueurl.show()



#PART4**************************************************************************************************************************************



#Find the most engaged users, ie the IPs with the longest session times by calculating the timetaken by each grouped by IP and useragent and sort it in descending to display the top most engaged users.
mostengagedusers = spark.sql("select sessionID, SUBSTR(clientip, 0, CHARINDEX(':', clientip)) clientip, useragent ,TIMETAKEN(max(substr(timestamp,12,8)),min(substr(timestamp,12,8))) time from session group by sessionID, SUBSTR(clientip, 0, CHARINDEX(':', clientip)), useragent order by time DESC")
mostengagedusers.show()




sc.stop()

