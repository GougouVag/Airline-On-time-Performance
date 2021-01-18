#Evangelos Gougoulis Dimitriadis

import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from functools import reduce
from pyspark.sql.functions import sum as _sum

start_time = time.time()
spark = SparkSession.builder.getOrCreate()

df = spark.read.csv(".../Project/data/data/*")

df_00 = df.select(df['_c0'].alias('Year') , \
 df['_c1'].alias('Month'), \
 df['_c2'].alias('DayofMonth'), \
 df['_c3'].alias('DayofWeek'), \
 df['_c4'].alias('DepartureTime(hh:mm)') ).dropna(how="all")

df_10 = df.select(df['_c11'].alias('ActualElapsedTime') , \
 df['_c12'].alias('CRSElapsedTime'), \
 df['_c8'].alias('UniqueCarrier')).dropna(how="all")

#OLD RQ-1
df_20 = df.select(df['_c3'].alias('DayofTheWeek')).dropna(how="all")
df_21 = df_20.groupBy(df_20.DayofTheWeek).count()
df_22 = df_21.sort("count", ascending=False)
df_23 = df_22.replace(['1', '2', '3', '4', '5', '6', '7'],\
 ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'], 'DayofTheWeek')
df_24 = df_23.filter("count > 1")

#RQ-2
Numb_of_days = 365 * 22 
df_30 = df.select(df['_c14'].alias('ArrDelay'), \
	df['_c15'].alias('DepDelay'), \
 	df['_c24'].alias('CarrierDelay'), \
 	df['_c25'].alias('WeatherDelay'), \
 	df['_c26'].alias('NASDelay'), \
 	df['_c27'].alias('SecurityDelay'), \
 	df['_c28'].alias('LateAircraftDelay'), \
 	df['_c16'].alias('AirportOrigin')).dropna(how="all")

df_31 = df_30.na.replace("NA", "0")
df_32 = df_31.select(df_31['AirportOrigin'], (col("ArrDelay") + col("DepDelay") + col("CarrierDelay") \
	+ col("WeatherDelay") + col("NASDelay") + col("SecurityDelay") + col("LateAircraftDelay")).alias("RowSumDelay")).dropna(how="all")
df_33 = df_32.na.replace("null" , "99999999")
df_34 = df_33.filter("RowSumDelay < 99999999")
df_35 = df_34.groupBy(df_34.AirportOrigin).agg(_sum('RowSumDelay').alias('AllSumDelayMinutes'))
df_36 = df_35.sort("AllSumDelayMinutes", ascending=False)
df_37 = df_36.select(df_36['AirportOrigin'], df_36['AllSumDelayMinutes'], (df_36['AllSumDelayMinutes']/Numb_of_days).alias("AvgDelayDaily"))
df_311 = df_34.groupBy(df_34.AirportOrigin).count()
df_38 = df_37.join(df_311, df_37.AirportOrigin == df_311.AirportOrigin).drop(df_311.AirportOrigin)
df_39 = df_38.select(df_38['AirportOrigin'],df_38['AvgDelayDaily'],(df_38['AvgDelayDaily']/df_38['count']).alias("AvgDelayDailyPerFlight"))
df_391 = df_39.sort("AvgDelayDaily", ascending=False)

#RQ-3
#HighestDelays ORD, ATL 
#LowestDelays ROP, VIS
list_of_Months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
list_of_Numbers = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']
df_40 = df.select(df['_c16'].alias('AirportOrigin'), \
	df['_c0'].alias('Year'), \
	df['_c1'].alias('Month'), \
	df['_c21'].alias('Cancelled'), \
	df['_c22'].alias('CancellationCode')).dropna(how="all")
df_41 = df_40.filter(("AirportOrigin = 'ATL'")) #for each of the prev airports??
#df_41.groupBy(df_41.AirportOrigin).count()
df_42 = df_41.filter(("Cancelled > 0"))                                                                                                                                                                                                                                                         ,
df_43 = df_42.filter(("CancellationCode = 'B'"))
df_44 = df_43.replace(list_of_Numbers, list_of_Months, 'Month') 



print(df_44.count())
with open(".../Project/output1_time.txt", 'w') as out_file:
     out_file.write("Duration: " + str(time.time() - start_time))
     out_file.write("\n")
     out_file.write("Number of lines: " + str(df_37.count()))
     out_file.write("\n")


#No need for extra headers (.header()) , UNCOMMENT next line for saving Dataframe in HDFS
df_44.coalesce(1).write.format("com.databricks.spark.csv").save(".../Project/outputs")


df_44.printSchema()
df_44.show()