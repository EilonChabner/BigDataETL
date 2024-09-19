
'''
Capitalize the names of the shifts (e.g. noon  →  Noon)
'''

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local").appName("eilon").getOrCreate()
dessert : DataFrame = spark.read.csv(r"class_example/data/dessert.csv", header = True, inferSchema = True)

# Read a csv file
dessert : DataFrame = spark.read.csv(r"class_example/data/dessert.csv",
                         header=True, inferSchema=True)\
                        .drop('id')\
                        .withColumnRenamed('day.of.week', 'weekday')\
                        .withColumnRenamed('num.of.guests', 'num_of_guests')\
                        .withColumnRenamed('dessert', 'purchase')\
                        .withColumnRenamed('hour', 'shift')


dessert = dessert.withColumn("shift",F.initcap("shift"))

dessert.show(3)