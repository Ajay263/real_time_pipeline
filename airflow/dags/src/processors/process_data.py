"""Module for processing sensor data using PySpark streaming."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, window
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, TimestampType
import yaml

def load_config():
    """Load configuration from YAML file."""
    with open('config/config.yml', 'r') as file:
        return yaml.safe_load(file)

def create_spark_session(config):
    """
    Create and return a Spark session.

    Args:
        config (dict): Configuration dictionary.

    Returns:
        SparkSession: Configured Spark session.
    """
    return (SparkSession.builder
            .appName(config['spark']['app_name'])
            .master(config['spark']['master'])
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3")
            .getOrCreate())

def process_stream(spark, config):
    """
    Process the streaming data from Kafka.

    Args:
        spark (SparkSession): Spark session.
        config (dict): Configuration dictionary.
    """
    schema = StructType([
        StructField("id", IntegerType()),
        StructField("temperature", FloatType()),
        StructField("humidity", FloatType()),
        StructField("timestamp", TimestampType())
    ])

    df = (spark
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", config['kafka']['bootstrap_servers'])
          .option("subscribe", config['kafka']['topic'])
          .load())

    parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # Process the data (calculate average temperature and humidity over a window)
    processed_df = (parsed_df
                    .groupBy(window(col("timestamp"), "1 minute"))
                    .agg(avg("temperature").alias("avg_temperature"),
                         avg("humidity").alias("avg_humidity")))

    # Write the processed data to PostgreSQL
    query = (processed_df
             .writeStream
             .outputMode("append")
             .format("jdbc")
             .option("url", f"jdbc:postgresql://{config['postgres']['host']}:{config['postgres']['port']}/{config['postgres']['database']}")
             .option("dbtable", "sensor_averages")
             .option("user", config['postgres']['user'])
             .option("password", config['postgres']['password'])
             .option("checkpointLocation", "/tmp/checkpoint")
             .start())

    query.awaitTermination()

if __name__ == "__main__":
    config = load_config()
    spark = create_spark_session(config)
    process_stream(spark, config)