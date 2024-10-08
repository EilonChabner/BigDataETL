from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F 
from pyspark.sql import types as T 
from pyspark.sql import Row


spark = SparkSession.builder.master('local[*]').config("spark.driver.memory", "4g").appName('googlePlay').getOrCreate()
google_apps_df : DataFrame = spark.read.csv("s3a://spark/data/raw/google_apps/", header= True)
google_apps_df.show(5)
age_limit_arr = [Row(age_limit=18, Content_Rating='Adults only 18+'),
                 Row(age_limit=17, Content_Rating='Mature 17+'),
                 Row(age_limit=12, Content_Rating='Teen'),
                 Row(age_limit=10, Content_Rating='Everyone 10+'),
                 Row(age_limit=0, Content_Rating='Everyone')]
         
age_limit_df = spark.createDataFrame(age_limit_arr).withColumnRenamed('Content_Rating', 'Content Rating')

age_limit_df.show(5)

joined_df = google_apps_df.join(F.broadcast(age_limit_df), ['Content Rating'])
selected_df = joined_df\
        .select(F.col('App').alias('application_name'),
                F.col('Category').alias('category'),
                F.col('Rating').alias('rating'),
                F.col('Reviews').cast(T.FloatType()).alias('reviews'),
                F.col('Size').alias('size'),
                F.regexp_replace(F.col('Installs'), '[^0-9]', '').cast(T.DoubleType()).alias('num_of_installs'),
                F.col('Price').cast(T.DoubleType()).alias('price'),
                F.col('age_limit'),
                F.col('Genres').alias('genres'),
                F.col('Current Ver').alias('version'))\
        .fillna(-1, 'reviews')

joined_df.show(5)

selected_df.show(5)

selected_df.write.parquet('s3a://spark/data/source/google_apps', mode='overwrite')