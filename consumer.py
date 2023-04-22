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

schema_id = StructType([
    StructField("id", StringType(), True)
    
])

schema_sentiment = StructType([StructField("sentiment", StringType(), True)])

schema_hashtag = StructType([StructField("hashtag", StringType(), True)])

spark = SparkSession \
    .builder \
    .appName("KafkaStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


sentiment_dict = {"positive": 0, "negative": 0}

hash_tag_dict = dict()

tweet_count = 0


df_id = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "id") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_id).alias("data")) \
    .select("data.*")

df_hashtag = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "hashtag") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_hashtag).alias("data")) \
    .select("data.*")

df_sentiment = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sentiment") \
    .option("startingOffsets", "earliest") \
    .option("kafka.group.id", "consumer")\
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_sentiment).alias("data")) \
    .select("data.*")


def process_batch_hashtag(batch_df, batch_id):
    global hash_tag_dict
    hash_tag_dict_l = (
        batch_df.groupby('hashtag')
        .count()
        .rdd.map(lambda x: (x['hashtag'], x['count']))
        .collectAsMap()
    )
    for i in hash_tag_dict_l.keys():
        if i not in hash_tag_dict:
            hash_tag_dict[i] = hash_tag_dict_l[i]
        else:
            hash_tag_dict[i] += hash_tag_dict_l[i]
    mydb["hash_tags"].delete_many({})
    mydb["hash_tags"].insert_one(hash_tag_dict)

def process_batch_tweet(batch_df, batch_id):
    global tweet_count
    tweet_count += batch_df.count()

def process_batch_sentiment(batch_df, batch_id):
    # Get the sentiment count as a dictionary
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
    df_sentiment.writeStream
    .foreachBatch(process_batch_sentiment)
    .start()
)

query_hashtag = (
    df_hashtag.writeStream
    .foreachBatch(process_batch_hashtag)
    .start()
)
query_count_tweet = (
    df_hashtag.writeStream
    .foreachBatch(process_batch_tweet)
    .start()
)

query_sentiment.awaitTermination()
query_hashtag.awaitTermination()
query_count_tweet.awaitTermination()

spark.stop()

