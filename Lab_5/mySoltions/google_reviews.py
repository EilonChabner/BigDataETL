from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F 
from pyspark.sql import types as T 
from pyspark.sql import Row


spark = SparkSession.builder.master("local[*]").appName('ex5_google_reviews').getOrCreate()

google_reviews_df : DataFrame = spark.read.csv("s3a://spark/data/raw/google_reviews/", header= True)

google_reviews_df.show(5)

sentiment_arr = [Row(Sentiment='Positive', sentiment_rank=1),
                 Row(Sentiment='Neutral', sentiment_rank=0),
                 Row(Sentiment='Negative', sentiment_rank=-1)]

sentiments_df = spark.createDataFrame(sentiment_arr)

joined_df = google_reviews_df.join(F.broadcast(sentiments_df), ['Sentiment'])

selected_df = joined_df\
    .select(F.col('App').alias('application_name'),
            F.col('Translated_Review').alias('translated_review'),
            F.col('sentiment_rank'),
            F.col('Sentiment_Polarity').cast(T.FloatType()).alias('sentiment_polarity'),
            F.col('Sentiment_Subjectivity').cast(T.FloatType()).alias('sentiment_subjectivity'))

selected_df.show(5)
selected_df.write.parquet('s3a://spark/data//source/google_reviews', mode='overwrite')

spark.stop()