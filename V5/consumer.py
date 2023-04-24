from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pymongo
import json
from kafka import KafkaProducer

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["Kafka"]
mycol = mydb["traffic_data"]

spark = SparkSession.builder.appName("Traffic").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

schema = StructType([
    StructField("id",StringType(), True),
    StructField("datetime",StringType(), True),
    StructField("issue",StringType(), True)
])

aggregate_count = {"Traffic Hazard":0, "Crash Urgent": 0, "Crash Service": 0, "COLLISION": 0}

def insert_to_mongo(df, epoch_id):
    # Convert dataframe to list of dictionaries
    data = df.toJSON().map(lambda j: json.loads(j)).collect()

    # Insert each row into MongoDB collection
    for row in data:
        mycol.insert_one(row)

def update_issue_counts(df, epoch_id):
    # Aggregate the counts for each issue
    counts = df.groupBy("issue").count().collect()
    for row in counts:
        issue = row["issue"]
        count = row["count"]
        aggregate_count[issue] += count
    # Publish updated count
    producer.send("aggregate_count", value = aggregate_count)
    print(aggregate_count)
    


df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","TrafficHazard,CrashUrgent,CrashService,COLLISION") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

raw_df = df.selectExpr("CAST(value AS STRING)")

s_df = raw_df.select(from_json(raw_df.value, schema).alias("data"))

traffic_df = s_df.select(col("data.id"),col("data.datetime"),col("data.issue"))
insert_to = traffic_df.writeStream.foreachBatch(insert_to_mongo).start()

output_df = traffic_df.select(to_json(struct(col("*"))).alias("value"))

op = traffic_df.writeStream.format("console").foreachBatch(update_issue_counts).outputMode("append").start()

query = output_df.writeStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("checkpointLocation", "/tmp/spark_checkpoint") \
    .option("topic", "output") \
    .outputMode("update") \
    .start()

query.awaitTermination()
insert_to.awaitTermination()
