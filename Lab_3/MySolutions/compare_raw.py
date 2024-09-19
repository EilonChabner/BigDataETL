from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from datetime  import date, datetime, timedelta
from pyspark.sql import Row

spark = SparkSession.builder.master("local[*]").config("spark.driver.memory", "4g").appName('ex3_clean_flights').getOrCreate()

flights_data : DataFrame = spark.read.parquet('s3a://spark/data/source/flights/')
flights_raw_data : DataFrame = spark.read.parquet('s3a://spark/data/source/flights_raw/')

print(flights_data.count())
print(flights_raw_data.count())

distinct_flights_data = flights_data.dropDuplicates()
distinct_flights_raw_data = flights_raw_data.dropDuplicates()


print(distinct_flights_data.count())
print(distinct_flights_raw_data.count())

match_data = distinct_flights_data.intersect(distinct_flights_raw_data)

unmatch_data_flights_raw = distinct_flights_raw_data.subtract(distinct_flights_data).withColumn("source_of_data", F.lit("flights_raw"))
unmatch_data_flights = distinct_flights_data.subtract(distinct_flights_raw_data).withColumn("source_of_data", F.lit("flights"))

unmatched_df = unmatch_data_flights_raw.union(unmatch_data_flights)

unmatched_df.show(50)


match_data.write.parquet('s3a://spark/data/stg/flight_matched/', mode='overwrite')
unmatched_df.write.parquet('s3a://spark/data/stg/flight_unmatched/', mode='overwrite')


spark.stop()




