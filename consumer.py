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
          
          
          
sentiment_count = dict()
def process_batch(batch_df, batch_id):
    # Get the sentiment count as a dictionary
    global sentiment_count
    sentiment_count = (
        batch_df.groupBy('Sentiment')
        .count()
        .rdd.map(lambda x: (x['Sentiment'], x['count']))
        .collectAsMap()
    )

app = Flask(__name__)
# Define the route to the dashboard
@app.route("/")
def dashboard():
	# Read data from Kafka


	# Convert the sentiment count DataFrame to a Pandas DataFrame
	sentiment_count_pd = pd.DataFrame.from_dict(sentiment_count)

	# Create a pie chart of the sentiment count
	plt.pie(sentiment_count_pd["count"], labels=["Negative", "Positive"])
	plt.title("Sentiment Analysis")
	plt.show()

	# Render the HTML template with the pie chart and tweet count
	return render_template("dashboard.html", sentiment_count=sentiment_count_pd, tweet_count=0)
     
t1 = Thread(target = app.run, kwargs={'host': 'localhost', 'port': 5000})
t1.start()     

query_sentiment = (
    df.writeStream
    .foreachBatch(process_batch)
    .start()
)

query_sentiment.awaitTermination()

query.awaitTermination()
spark.stop()

