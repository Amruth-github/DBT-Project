from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import SparkSession
from flask import Flask, render_template
from threading import *
import pandas as pd
schema = StructType([
    StructField("id", StringType(), True),
    StructField("sentiment", StringType(), True),
    StructField("hashtag", StringType(), True)
])
spark = SparkSession \
    .builder \
    .appName("KafkaStreaming") \
    .getOrCreate()
   
spark.sparkContext.setLogLevel("WARN")

df = spark.readStream \
         .format("kafka") \
         .option("kafka.bootstrap.servers", "localhost:9092") \
         .option("subscribe", "sentiment") \
         .option("startingOffsets", "earliest") \
         .load() \
         .selectExpr("CAST(value AS STRING)") \
         .select(from_json(col("value"), schema).alias("data")) \
         .select("data.*")


query = df.writeStream \
          .format("console") \
          .outputMode("append") \
          .start()

print(query);
          
          
sentiment_count = dict()
def process_batch(batch_df, batch_id):
    # Get the sentiment count as a dictionary
    global sentiment_count
    sentiment_count = (
        batch_df.groupBy('hashtag')
        .count()
        .rdd.map(lambda x: (x['hashtag'], x['count']))
        .collectAsMap()
    )
    print(sentiment_count);
   

query_sentiment = (
    df.writeStream
    .foreachBatch(process_batch)
    .start()
)

query_sentiment.awaitTermination()

query.awaitTermination()
spark.stop()

