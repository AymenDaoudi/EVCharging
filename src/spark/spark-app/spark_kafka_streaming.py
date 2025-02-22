import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
import os

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
CHARGING_EVENTS_TOPIC = os.getenv('CHARGING_EVENTS_TOPIC', 'charging_events')

logger.info("Starting Spark Streaming Job")

# Create a Spark session
# Create a Spark session with network configurations
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.shuffle.partitions", "1") \
    .config("spark.network.timeout", "800s") \
    .config("spark.rpc.message.maxSize", "1024") \
    .config("spark.driver.maxResultSize", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

logger.info("Spark Session Created")  

logger.info("Reading from Kafka")

try:
    # Read stream from Kafka with smaller batch size
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", CHARGING_EVENTS_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 1000) \
        .option("failOnDataLoss", "false") \
        .load()

    logger.info("Successfully connected to Kafka topic")

    # Select relevant fields and cast them appropriately
    df = df.selectExpr(
        "CAST(key AS STRING) as key",
        "CAST(value AS STRING) as value",
        "topic",
        "partition",
        "offset",
        "timestamp"
    )

    # Write output to console with trigger
    query = df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='5 seconds') \
        .start()

    logger.info("Query Started")
    query.awaitTermination()
    
    logger.info("Query Ended")

except Exception as e:
    logger.error(f"An error occurred: {str(e)}")
    spark.stop()