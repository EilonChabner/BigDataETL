'''
How many visitors purchased a dessert?
What is the average table?
'''

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local").appName("eilon").getOrCreate()


dessert : DataFrame = spark.read.csv(r"class_example/data/dessert.csv", header = True, inferSchema = True)

dessert.show(3)

dessert = dessert.withColumnRenamed("num.of.guests","num_of_guests")

dessert \
    .where(dessert.dessert)\
    .agg({'num_of_guests':'sum', 'table' : 'mean'})\
    .withColumnRenamed("sum(num_of_guests)","Total_Guests")\
    .withColumnRenamed("avg(table)","avg_table")\
    .show()

print('##############################')

dessert.show(3)

dessert \
    .where(dessert.dessert)\
    .agg(F.sum('num_of_guests').alias("Total_Guests"),
         F.avg('table').alias("avg_table"))\
    .show()



