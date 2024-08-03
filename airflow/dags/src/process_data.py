from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, TimestampType

def process_kafka_data(bootstrap_servers, topic):
    spark = SparkSession.builder \
        .appName("KafkaSparkStreaming") \
        .getOrCreate()

    # Define the schema for the incoming JSON data
    schema = StructType([
        StructField("id", IntegerType()),
        StructField("temperature", FloatType()),
        StructField("humidity", FloatType()),
        StructField("timestamp", TimestampType())
    ])

    # Read from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .load()

    # Parse JSON data
    parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # Process the data (example: calculate average temperature and humidity)
    result_df = parsed_df \
        .groupBy() \
        .agg({"temperature": "avg", "humidity": "avg"})

    # Write the results to the console (you can change this to write to a database or file)
    query = result_df \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    bootstrap_servers = "broker:29092"
    topic = "sensor_data"
    process_kafka_data(bootstrap_servers, topic)