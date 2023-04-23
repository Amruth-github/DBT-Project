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

print(output_df)

query = output_df.writeStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("checkpointLocation", "/tmp/spark_checkpoint") \
    .option("topic", "output") \
    .outputMode("update") \
    .start()

query.awaitTermination()