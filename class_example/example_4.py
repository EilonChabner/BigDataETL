'''
How many groups purchased a dessert on Mondays?
'''

from pyspark.sql import SparkSession, DataFrame

spark = SparkSession.builder.master("local").getOrCreate()
spark = SparkSession.builder.master("local").appName('ex2_flights').getOrCreate()

dessert : DataFrame = spark.read.csv(r"class_example/data/dessert.csv", header = True, inferSchema = True) 

dessert.show(3)

dessert = dessert\
    .withColumnRenamed("day.of.week","weekday")

dessert.show(3)

con = (dessert.dessert) & (dessert.weekday == "Monday")

print(dessert.where(con).count())



