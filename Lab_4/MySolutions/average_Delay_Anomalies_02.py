from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F 
from pyspark.sql import types as T 
from pyspark.sql import Window

spark = SparkSession.builder.master('local[*]').config("spark.driver.memory", "4g").appName('ex4_anomalies_detection').getOrCreate()

unbounded_window = Window.partitionBy(F.col("Carrier")).rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)

flights_df : DataFrame = spark.read.parquet("s3a://spark/data/transformed/flights/")

flights_df.cache()

flights_df.withColumn("avg_all_time", F.avg(F.col('arr_delay')).over(unbounded_window))\
          .withColumn("avg_diff_percent", F.abs(F.col("arr_delay") / F.col("avg_all_time")))\
          .where(F.col("avg_diff_percent") > F.lit(5.0))\
          .show()


flights_df.unpersist()

spark.stop()