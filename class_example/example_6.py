'''
For each weekday - how many groups purchased a dessert?
'''

from pyspark.sql import SparkSession, DataFrame

spark = SparkSession.builder.master("local").appName("eilon").getOrCreate()


dessert : DataFrame = spark.read.csv(r"class_example/data/dessert.csv", header = True, inferSchema = True)

dessert.show(3)

dessert = dessert.withColumnRenamed("day.of.week","weekday")\
                 .withColumnRenamed("num.of.guests","num_of_guests")
          

dessert.where("dessert").groupBy("weekday").agg({"num_of_guests":"sum"}).show()