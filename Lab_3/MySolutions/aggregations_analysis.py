from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = SparkSession.builder.getOrCreate()

flights_df : DataFrame = spark.read.parquet('s3a://spark/data/transformed/flights/')
airports_df : DataFrame = spark.read.parquet('s3a://spark/data/source/airports/')

flights_df.printSchema()
airports_df.printSchema()

flights_df.show(5)
airports_df.show(5)

# flights_df.groupBy('origin_airport_id')\
#                    .agg(F.count('origin_airport_id').alias('number_of_departures'))\
#                    .withColumnRenamed("origin_airport_id","airport_id")\
#                    .orderBy(F.col('number_of_departures').desc())\
#                    .show()

# flights_df.groupBy(F.col('origin_airport_id').alias('airport_id')) \
#     .agg(F.count('origin_airport_id').alias('number_of_departures')) \
#     .join(airports_df,'airport_id')\
#     .select(F.col('name').alias('airport_name'),'number_of_departures')\
#     .orderBy(F.col('number_of_departures').desc()) \
#     .show(10)


# flights_df.groupBy(F.col('dest_airport_id').alias('airport_id'))\
#                    .agg(F.count(F.col('dest_airport_id')).alias('number_of_arr'))\
#                    .join(airports_df,'airport_id')\
#                    .select(F.col('name').alias('airport_name'),'number_of_arr')\
#                    .orderBy(F.col('number_of_arr').desc())\
#                    .show(10)


flights_df.groupBy(F.col('origin_airport_id').alias('source_airport_id'),'dest_airport_id')\
                   .agg(F.count('*').alias('number_of_taken'))\
                   .select('source_airport_id','dest_airport_id','number_of_taken')\
                   .join(airports_df.select(F.col('airport_id').alias('source_airport_id'), F.col('name').alias('source_airport_name')), 'source_airport_id')\
                   .join(airports_df.select(F.col('airport_id').alias('dest_airport_id'), F.col('name').alias('dest_airport_name')), 'dest_airport_id')\
                   .select(F.concat_ws('->',F.col('source_airport_name'),F.col('dest_airport_name')).alias('Route'), F.col('number_of_taken'))\
                   .orderBy(F.col('number_of_taken').desc())\
                   .show(10,False)


spark.stop()