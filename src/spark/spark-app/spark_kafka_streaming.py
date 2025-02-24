import logging
from pyspark.sql.functions import from_json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, MapType
import os

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment variables
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
CHARGING_EVENTS_TOPIC = os.getenv('CHARGING_EVENTS_TOPIC', 'charging_events')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')

schema = StructType([
    StructField("event_type", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("station_id", StringType(), True),
    StructField("ev_id", StringType(), True),
    StructField("payload", MapType(StringType(), StringType()), True)
])

logger.info("Starting Spark Streaming Job")

# Initialize Spark with Kafka packages
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

logger.info("Spark Session Created")  

try:
    # Test connection to MinIO
    test_df = spark.createDataFrame([("test",)], ["col1"])
    test_path = "s3a://raw-data/test"
    
    test_df.write.mode("overwrite").parquet(test_path)
    logger.info("Successfully wrote test data to MinIO")
    
    # Try reading it back
    test_read = spark.read.parquet(test_path)
    logger.info("Successfully read test data from MinIO")
    logger.info(f"Test data count: {test_read.count()}")

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
        col("data.station_id").alias("station_id"),
        col("data.ev_id").alias("ev_id"),
        col("data.payload").alias("payload"),
    )

    logger.info("Successfully connected to Kafka topic")
    
    # Write to Azure Blob Storage
    blob_query = df.writeStream \
        .format("parquet") \
        .option("path", "s3a://raw-data/charging-events") \
        .option("checkpointLocation", "s3a://checkpoints/charging-events") \
        .partitionBy("station_id") \
        .trigger(processingTime='5 seconds') \
        .outputMode("append") \
        .start()
    
    logger.info("Queries Started")
    
    # Wait for both queries to terminate
    # console_query.awaitTermination()
    blob_query.awaitTermination()
    logger.info("Queries Ended")

except Exception as e:
    logger.error(f"An error occurred: {str(e)}")
    spark.stop()