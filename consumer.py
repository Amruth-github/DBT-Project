from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Traffic").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("id",StringType(), True),
    StructField("datetime",StringType(), True),
    StructField("issue",StringType(), True)
])

aggregate_count = {"Traffic Hazard":0, "Crash Urgent": 0, "Crash Service": 0, "COLLISION": 0}

def update_issue_counts(df, epoch_id):
    # Aggregate the counts for each issue
    counts = df.groupBy("issue").count().collect()
    for row in counts:
        issue = row["issue"]
        count = row["count"]
        aggregate_count[issue] += count
    # Print the updated counts
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

output_df = traffic_df.select(to_json(struct(col("*"))).alias("value"))

op = traffic_df.writeStream.format("console").foreachBatch(update_issue_counts).outputMode("append").start()

query = output_df.writeStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("checkpointLocation", "/tmp/spark_checkpoint") \
    .option("topic", "output") \
    .outputMode("update") \
    .start()

query.awaitTermination()
