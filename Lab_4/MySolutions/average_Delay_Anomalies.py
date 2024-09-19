from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window

spark = SparkSession.builder.master("local[*]").config("spark.driver.memory", "4g").getOrCreate()

flights_df : DataFrame = spark.read.parquet('s3a://spark/data/transformed/flights/')

flights_df.cache()

flights_df.show()


window = Window.partitionBy(F.col("carrier")).orderBy(F.col("flight_date"))




flights_df\
    .withColumn("avg_till_now",F.avg(F.col('arr_delay')).over(window))\
    .withColumn("avg_diff_percent",F.abs(F.col("arr_delay") / F.col("avg_till_now")))\
    .where(F.col("avg_diff_percent") > F.lit(3.0))\
    .show()
    

flights_df.unpersist()

spark.stop()