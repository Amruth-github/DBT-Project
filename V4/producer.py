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


def publish_tweet(data,topic):
    producer.send(topic, value=data)


# Read tweets from a file or stream
if __name__ == '__main__':
    with open("./traffic.csv", "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if str(row["Issue Reported"]) in ["Traffic Hazard","Crash Urgent","Crash Service","COLLISION"]:
                data = {
                    "id": str(row["Traffic Report ID"]),
                    "datetime": str(row["Published Date"]),
                    "issue": str(row["Issue Reported"]),
                }
                print(data)
                publish_tweet(data,data["issue"].replace(" ",""))
                sleep(1)

