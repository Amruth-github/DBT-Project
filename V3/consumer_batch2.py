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
    counts = df.groupBy("issue").sum("count").collect()
    for row in counts:
        issue = row["issue"]
        count = row["sum(count)"]
        print(issue,count)
        aggregate_count[issue] += count
    # Print the updated counts
    print(aggregate_count)


traffic_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","TrafficHazard,CrashUrgent,CrashService,COLLISION") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("includeTimestamp", "true") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select(col("data.id"), col("data.datetime"), col("data.issue"))



# Tumbling window of 10 seconds
windowed_traffic_df = traffic_df \
    .groupBy(window(current_timestamp(), "10 seconds"), col("issue")) \
    .count()

    
query = windowed_traffic_df.writeStream \
          .format("console") \
          .outputMode("complete") \
          .start()

output_df = windowed_traffic_df \
    .select(to_json(struct(col("*"))).alias("value"))

op = windowed_traffic_df.writeStream.format("console") \
    .outputMode("complete") \
    .foreachBatch(update_issue_counts) \
    .start()

op.awaitTermination()

