from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from datetime  import date, datetime, timedelta
from pyspark.sql import Row
from pyspark.sql import types as T

spark = SparkSession.builder.master("local[*]").appName('ex3_add_dates').getOrCreate()

def get_dates_df():
    dummy_df = spark.createDataFrame([Row(dummy='x')])
    in_dates_df = dummy_df.select(F.explode(F.sequence(F.lit("2020-01-01").cast(T.DateType()), F.lit("2020-12-31").cast(T.DateType()))).alias("flight_date"))
    return in_dates_df


filights_df : DataFrame = spark.read.parquet('s3a://spark/data/stg/flight_matched/')

date_df : DataFrame = get_dates_df()

date_df.show(20)

# date_df = date_df.select(F.col('flight_date').dayofweek.alias('day_of_week'),
#                          F.col('flight_date').daysofmonth.alias('day_of_month'))

date_df = date_df\
    .withColumn("day_of_week" ,F.dayofweek('flight_date'))\
    .withColumn('day_of_month', F.dayofmonth('flight_date'))

# dates_full_df = dates_df \
#     .withColumn('day_of_week', F.dayofweek(F.col('flight_date'))) \
#     .withColumn('day_of_month', F.dayofmonth(F.col('flight_date')))


date_df.show(5)

max_date_df : DataFrame = date_df \
                            .groupBy(F.col("day_of_week") , F.col("day_of_month"))\
                            .agg(F.max(F.col("flight_date")).alias('flight_date'))


max_date_df.show(3)


filights_df = filights_df.join(max_date_df, ['day_of_week','day_of_month'])

filights_df.show()

filights_df.write.parquet("s3a://spark/data/transformed/flights/", mode= 'overwrite')


spark.stop()
