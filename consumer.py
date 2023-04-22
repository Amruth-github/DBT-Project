from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import SparkSession
from flask import Flask, render_template
from threading import *
import pandas as pd
import pymongo
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import numpy as np
import matplotlib
matplotlib.use('TkAgg')

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["Kafka"]
mycol = mydb["Batch_results"]

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


sentiment_dict = {"positive": 0, "negative": 0}

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

print(query)


sentiment_count = dict()


def process_batch(batch_df, batch_id):
    # Get the sentiment count as a dictionary
    global sentiment_count
    sentiment_count = (
        batch_df.groupBy('sentiment')
        .count()
        .rdd.map(lambda x: (x['sentiment'], x['count']))
        .collectAsMap()
    )
    for i in sentiment_count.keys():
        if i == "0":
            sentiment_dict["positive"] += int(sentiment_count[i])
        else:
            sentiment_dict["negative"] += int(sentiment_count[i])
        to_insert = dict(sentiment_count)
        to_insert["Batch-id"] = batch_id
        mycol.insert_one(to_insert)
    
query_sentiment = (
    df.writeStream
    .foreachBatch(process_batch)
    .start()
)

query_sentiment.awaitTermination()

query.awaitTermination()
spark.stop()

