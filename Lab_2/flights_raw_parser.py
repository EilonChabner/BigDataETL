from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
 

spark = SparkSession.builder.master("local").appName('ex2_flights').getOrCreate()

flights_raw_df: DataFrame = spark.read.csv('s3a://spark/data/raw/flights_raw/', header=True)

flights_raw_df.show(3)

# flights_df = flights_raw_df\
#     .withColumn("DayofMonth",flights_raw_df["DayofMonth"].cast("integer"))\
#     .withColumnRenamed("DayofMonth","day_of_month")\
#     .withColumn("DayOfWeek",flights_raw_df["DayOfWeek"].cast("integer"))\
#     .withColumnRenamed("DayOfWeek","day_of_week")\
#     .withColumnRenamed("Carrier","carrier")\
#     .withColumn("OriginAirportID",flights_raw_df["OriginAirportID"].cast("integer"))\
#     .withColumnRenamed("OriginAirportID","origin_airport_id")\
#     .withColumn("DestAirportID",flights_raw_df["DestAirportID"].cast("integer"))\
#     .withColumnRenamed("DestAirportID","dest_airport_id")\
#     .withColumn("DepDelay",flights_raw_df["DepDelay"].cast("integer"))\
#     .withColumnRenamed("DepDelay","dep_delay")\
#     .withColumn("ArrDelay",flights_raw_df["ArrDelay"].cast("integer"))\
#     .withColumnRenamed("ArrDelay","arr_delay")\
    
flight_df = flights_raw_df.select(
    F.col('DayofMonth').cast(T.IntegerType()).alias('day_of_month'),
    F.col('DayOfWeek').cast(T.IntegerType()).alias('day_of_week'),
    F.col('Carrier').alias('carrier'),
    F.col('OriginAirportID').cast(T.IntegerType()).alias('origin_airport_id'),
    F.col('DestAirportID').cast(T.IntegerType()).alias('dest_airport_id'),
    F.col('DepDelay').cast(T.IntegerType()).alias('dep_delay'),
    F.col('ArrDelay').cast(T.IntegerType()).alias('arr_delay'))
    


flight_df.show(3)

flight_df.write.parquet('s3a://spark/data/source/flights_raw/',mode='overwrite')

spark.stop()
