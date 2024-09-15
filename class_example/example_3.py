'''
How many groups purchased a dessert?
'''

from pyspark.sql import SparkSession, DataFrame

spark = SparkSession.builder.getOrCreate()

dessert : DataFrame = spark.read.csv(r"class_example/data/dessert.csv", header = True , inferSchema=True)

dessert.show(3)

print(dessert.where(dessert.dessert).count())