from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count, window

#Initilaize Spark Session
spark = SparkSession.builder \
    .appName("RealTimeProcessing") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
    .getOrCreate()

#Define schema
schema = """
    player_id STRING,
    game_id STRING,
    action_type STRING
    timestamp STRING,
    metrics STRUCT<reaction_time_ms: INT, accuracy: DOUBLE>
    location STRUCT<lat: DOUBLE, lon: DOUBLE>
"""

#Read streaming data from Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "raw_events") \
    .load()

#Parse the data
parsed_stream = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data"))

#Process the data: Aggregation by game_id
aggregated_stream = parsed_stream.groupBy(
    col("data.game_id"),
    window(col("data.timestamp").cast("timestamp"), "1 minute")
).agg(
    avg("data.metrics.reaction_time_ms").alias("avg_reaction_time"),
    count("data.action_type").alias("actions_per_min")
)

#Write processed data back to Kafka
query = aggregated_stream.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "processed_metrics") \
    .outputMode("complete") \
    .start()

query.awaitTermination()