from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types



spark=SparkSession.builder.master("local").appName('ex2_airports').getOrCreate()

airports_raw_df = spark.read.csv('s3a://spark/data/raw/airports/', header=True)

#airports_raw_df.show(3)
airports_raw_df.printSchema()

airports_df = airports_raw_df\
    .withColumn("airport_id",airports_raw_df["airport_id"].cast("integer"))

airports_df.printSchema()
airports_df.show(3)

airports_df.write.parquet('s3a://spark/data/source/airports/',mode='overwrite')