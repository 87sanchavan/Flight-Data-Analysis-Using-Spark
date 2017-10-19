import os
import sys

os.environ['SPARK_HOME'] = "/opt/spark/2.0.2"
sys.path.append("/opt/spark/2.0.2/python")

from pyspark import SparkContext
from  pyspark.sql import SQLContext
from pyspark import  SparkConf
from pyspark.sql.functions import *
from pyspark.sql.types import *

conf = SparkConf().setAppName("Flight Delay Per Origin for SequenceFile Format").set("spark.ui.port",5050)
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

rawFlightDataRDD = sc.sequenceFile(sys.argv[1], valueClass="org.apache.hadoop.io.Text")
rawFlightDataRDD = rawFlightDataRDD.map(lambda (k,v):(v[1:-1].split(",")))
flightDataDF = rawFlightDataRDD.toDF(['Year', 'Month', 'DayofMonth', 'DayOfWeek', 'DepTime', 'CRSDepTime',
                                     'ArrTime', 'CRSArrTime', 'UniqueCarrier', 'FlightNum', 'TailNum', 'ActualElapsedTime',
                                     'CRSElapsedTime', 'AirTime', 'ArrDelay', 'DepDelay', 'Origin', 'Dest', 'Distance', 'TaxiIn',
                                     'TaxiOut', 'Cancelled', 'CancellationCode', 'Diverted', 'CarrierDelay', 'WeatherDelay', 'NASDelay',
                                     'SecurityDelay', 'LateAircraftDelay'])

flightDelayDataDF = flightDataDF.withColumn("isDelayed", when(col("DepDelay") > 0, 1).otherwise(0))
flightDelayPerOriginDF = flightDelayDataDF.groupBy("Origin").agg(((sum("isDelayed")/count('*')) * 100).alias("Departure Delay in Percentage")).orderBy("Origin")
flightDelayPerOriginDF.rdd.saveAsTextFile(sys.argv[2])
