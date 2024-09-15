'''
Each record in the "dessert" dataset describes a group visit at a restaurant.
Read the data and answer the questions below.
drop the id
change columns:
'day.of.week' -> 'weekday'
'num.of.guest's -> 'num_of_guests'
'dessert' -> 'purchase'
'hour' ->  'shift'
'''

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = SparkSession.builder.getOrCreate()

dessert : DataFrame =  spark.read.csv(r"class_example/data/dessert.csv",header=True, inferSchema=True)

dessert.show(5)

dessert_new = dessert.select(
    F.col("`day.of.week`").alias("weekday"),
    F.col("`num.of.guests`").alias("num_of_guests"),
    F.col("dessert").alias("purchase"),
    F.col("hour").alias("shift")
)

dessert_new.show(5)

spark.stop()