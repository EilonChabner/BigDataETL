'''
How many line we have in dataframe?

'''

from pyspark.sql import SparkSession


spark = SparkSession.builder.getOrCreate()

dessert = spark.read.csv(r"class_example/data/dessert.csv",header=True, inferSchema=True)

print(dessert.count())

