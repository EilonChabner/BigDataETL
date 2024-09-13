from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
 

spark = SparkSession.builder.master("local").appName('ex2_flights').getOrCreate()

flights_raw_df: DataFrame = spark.read.csv('s3a://spark/data/raw/flights/', header=True)

flights_raw_df.show(3)

flights_raw_df = flights_raw_df\
    .withColumnRenamed("DayofMonth","day_of_month")\
    .withColumnRenamed("DayOfWeek","day_of_week")\
    .withColumnRenamed("Carrier","carrier")\
    .withColumn("OriginAirportID",flights_raw_df["OriginAirportID"].cast("integer"))\
    .withColumnRenamed("OriginAirportID","origin_airport_id")\
    .withColumn("DestAirportID",flights_raw_df["DestAirportID"].cast("integer").alias("dest_airport_id"))\
    .withColumn("DepDelay",flights_raw_df["DepDelay"].cast("integer").alias("dep_delay"))\
    .withColumn("ArrDelay",flights_raw_df["ArrDelay"].cast("integer").alias("arr_delay"))\
    

flights_raw_df.show(3)

