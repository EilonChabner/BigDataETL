'''
Add to dessert a new column called 'no purchase' with the negative of 'purchse'.
'''

from pyspark.sql import SparkSession, DataFrame

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

dessert = dessert.withColumn('NoPurchase',~dessert.purchase)
dessert.show(5)