from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F 
from pyspark.sql import types as T 
from pyspark.sql import Window

spark = SparkSession.builder.master('local[*]').config("spark.driver.memory", "4g").appName('ex4_anomalies_detection').getOrCreate()

sliding_range_window = Window.partitionBy('Carrier').orderBy('start_range')

flights_df : DataFrame = spark.read.parquet('s3a://spark/data/transformed/flights/')

flights_df.cache()

flights_df.withColumn("",F.sum(F.col('')).)