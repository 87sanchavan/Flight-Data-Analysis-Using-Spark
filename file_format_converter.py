import os
import sys

os.environ['SPARK_HOME'] = "/opt/spark/2.0.2"
sys.path.append("/opt/spark/2.0.2/python")

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark import  SparkConf
from pyspark.sql.functions import *
from pyspark.sql.types import *

conf = SparkConf().setAppName("Convert to 4 file formats").set("spark.ui.port",5050)
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

rawFlightDataDF = sqlContext.read.csv(sys.argv[1], header=True)
column_list = rawFlightDataDF.columns

for column in column_list:
    rawFlightDataDF = rawFlightDataDF.withColumn(column, when(col(column) == "NA", lit(None)).otherwise(col(column)))

flightDataDF = rawFlightDataDF\
                .withColumn("Year", rawFlightDataDF.Year.cast("int"))\
                .withColumn("Month", rawFlightDataDF.Month.cast("int"))\
                .withColumn("DayofMonth", rawFlightDataDF.DayofMonth.cast("int"))\
                .withColumn("DayOfWeek", rawFlightDataDF.DayOfWeek.cast("int"))\
                .withColumn("DepTime", rawFlightDataDF.DepTime.cast("int"))\
                .withColumn("CRSDepTime", rawFlightDataDF.CRSDepTime.cast("int")) \
                .withColumn("CRSArrTime", rawFlightDataDF.CRSArrTime.cast("int"))\
                .withColumn("ArrTime", rawFlightDataDF.ArrTime.cast("int"))\
                .withColumn("FlightNum", rawFlightDataDF.FlightNum.cast("int"))\
                .withColumn("ActualElapsedTime", rawFlightDataDF.ActualElapsedTime.cast("int"))\
                .withColumn("CRSElapsedTime", rawFlightDataDF.CRSElapsedTime.cast("int"))\
                .withColumn("AirTime", rawFlightDataDF.AirTime.cast("int"))\
                .withColumn("ArrDelay", rawFlightDataDF.ArrDelay.cast("int"))\
                .withColumn("DepDelay", rawFlightDataDF.DepDelay.cast("int"))\
                .withColumn("Distance", rawFlightDataDF.Distance.cast("int"))\
                .withColumn("TaxiIn", rawFlightDataDF.TaxiIn.cast("int"))\
                .withColumn("TaxiOut", rawFlightDataDF.TaxiOut.cast("int"))\
                .withColumn("CarrierDelay", rawFlightDataDF.CarrierDelay.cast("int"))\
                .withColumn("WeatherDelay", rawFlightDataDF.WeatherDelay.cast("int"))\
                .withColumn("NASDelay", rawFlightDataDF.NASDelay.cast("int"))\
                .withColumn("SecurityDelay", rawFlightDataDF.SecurityDelay.cast("int"))\
                .withColumn("LateAircraftDelay", rawFlightDataDF.LateAircraftDelay.cast("int"))

flightDataDF.write.csv(sys.argv[2], header=True)
flightDataDF.write.json(sys.argv[3])
flightDataDF.write.parquet(sys.argv[4])
flightDataDF.rdd.map(lambda t:(None, str(list(t)))).saveAsNewAPIHadoopFile(os.path.join(sys.argv[5], "sequencefile"),
                                                "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat")

