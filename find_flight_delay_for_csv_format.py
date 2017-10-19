import os
import sys

os.environ['SPARK_HOME'] = "/opt/spark/2.0.2"
sys.path.append("/opt/spark/2.0.2/python")

from pyspark import SparkContext
from  pyspark.sql import SQLContext
from pyspark import  SparkConf
from pyspark.sql.functions import *

conf = SparkConf().setAppName("Flight Delay Per Origin for CSV Format").set("spark.ui.port",5054)
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
flightDataDF = sqlContext.read.csv(sys.argv[1],header=True, inferSchema=True)
flightDelayDataDF = flightDataDF.withColumn("isDelayed", when(col("DepDelay") > 0, 1).otherwise(0))
flightDelayPerOriginDF = flightDelayDataDF.groupBy("Origin").agg(((sum("isDelayed")/count('*')) * 100).alias("Departure Delay in Percentage")).orderBy("Origin")
flightDelayPerOriginDF.rdd.saveAsTextFile(sys.argv[2])
