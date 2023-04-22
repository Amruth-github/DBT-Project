from kafka import KafkaProducer
import json
from time import sleep  
import csv



# # Set up the Kafka producer
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Define a function to publish tweets to Kafka


def publish_tweet(tweet):
    producer.send("sentiment", value=tweet)


# Read tweets from a file or stream
if __name__ == '__main__':
    with open("./tweets.csv", "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            tweet = {
                "id": str(row["ItemID"]),
                "sentiment": str(row["Sentiment"]),
                "hashtag": str(row["SentimentText"].split(" ")[-1])
            }
            print(tweet)
            publish_tweet(tweet)
            sleep(5)


