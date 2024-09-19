'''
Find the tables with the 5 highest percent of dessert buyers
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


res = dessert\
    .groupBy('table'),