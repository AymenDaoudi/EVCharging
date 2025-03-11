import logging
from pyspark.sql.functions import from_json, col
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, MapType, IntegerType
import os
from micro_batch_processor import write_to_branch_and_commit

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment variables
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
CHARGING_EVENTS_TOPIC = os.getenv('CHARGING_EVENTS_TOPIC', 'charging_events')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
REPOSITORY = os.getenv("LAKEFS_REPOSITORY", "charging-data")
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
LAKEFS_ENDPOINT = os.getenv("LAKEFS_ENDPOINT", "http://lakefs:8000")
LAKEFS_ACCESS_KEY = os.getenv("LAKEFS_ACCESS_KEY", "AKIAJBWUDLDFGJY36X3Q")
LAKEFS_SECRET_KEY = os.getenv("LAKEFS_SECRET_KEY", "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")

schema = StructType([
    StructField("session_id", StringType(), True),
    StructField("session_number", IntegerType(), True),
    StructField("station_id", StringType(), True),
    StructField("ev_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("payload", MapType(StringType(), StringType()), True)
])

def process_batch(data_frame, batch_id):
    
    logger.info(f"Processing batch {batch_id}")
    
    write_to_branch_and_commit(data_frame)
    
    logger.info(f"Finished processing batch {batch_id}")

logger.info("Starting Spark Streaming Job")

# Initialize Spark with Kafka packages
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
    .config("spark.hadoop.fs.s3a.access.key", LAKEFS_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", LAKEFS_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", LAKEFS_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

    # .config("spark.hadoop.fs.lakefs.impl", "io.lakefs.LakeFSFileSystem") \
    # .config("spark.hadoop.fs.lakefs.access.key", LAKEFS_ACCESS_KEY) \
    # .config("spark.hadoop.fs.lakefs.secret.key", LAKEFS_SECRET_KEY) \
    # .config("spark.hadoop.fs.lakefs.endpoint", LAKEFS_ENDPOINT) \

logger.info("Spark Session Created")  

try:
    
    logger.info("Reading from Kafka")
    # Read stream from Kafka
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", CHARGING_EVENTS_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 1000) \
        .option("failOnDataLoss", "false") \
        .load()

    # Extract JSON from Kafka message
    df = df_kafka.select(from_json(col("value").cast("string"), schema).alias("data"))

    # Flatten the structure
    df = df.select(
        col("data.event_type").alias("event_type"),
        col("data.session_id").alias("session_id"),
        col("data.session_number").alias("session_number"),
        col("data.station_id").alias("station_id"),
        col("data.ev_id").alias("ev_id"),
        col("data.payload").alias("payload")
    )

    logger.info("Successfully connected to Kafka topic")
    
    # Write to Azure Blob Storage
    blob_query = df.writeStream \
        .foreachBatch(process_batch) \
        .trigger(processingTime='5 seconds') \
        .start()
    
    logger.info("Queries Started")
    
    # Wait for both queries to terminate
    # console_query.awaitTermination()
    blob_query.awaitTermination()
    logger.info("Queries Ended")

except Exception as e:
    logger.error(f"An error occurred: {str(e)}")
    spark.stop()