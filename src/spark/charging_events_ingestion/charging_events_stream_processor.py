import logging
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import IntegerType, StructType, StructField, StringType
from lakefs_manager import create_branch, commit_to_branch
import logging
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import IntegerType
import os

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment variables
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
CHARGING_EVENTS_TOPIC = os.getenv('CHARGING_EVENTS_TOPIC', 'charging_events')
REPOSITORY = os.getenv("LAKEFS_REPOSITORY", "charging-data")

session_branches: dict[int, str] = {}

def range_resolver(session_number: int):
    range = 1000
    i = 1;
    
    while range * i < session_number:
        i += 1
        
    return range * i

def get_session_number_ranges(df):
    range_resolver_udf = udf(range_resolver, IntegerType())
    df = df.withColumn("session_number_range", range_resolver_udf(col("session_number")))
    
    # Get distinct session number ranges
    session_number_ranges = [row.session_number_range for row in df.select("session_number_range").distinct().collect()]
    return df, session_number_ranges

def get_branch(session_number_range):
    branch: str | None = None 
    if session_number_range not in session_branches:
        branch = create_branch(REPOSITORY, f"batch_{session_number_range}")
        logger.info(f"🔀 Created branch {branch}")
        assert branch is not None, "Failed to create branch"
        session_branches[session_number_range] = branch
    else:
        branch = session_branches[session_number_range]
    return branch

def write_to_branch_and_commit(df):
    
    # Get session number ranges belonging to the data frame, example: 1000, 2000, 3000, 4000, 5000
    df, session_number_ranges = get_session_number_ranges(df)
    
    logger.info(f"Found {len(session_number_ranges)} session number ranges.")
    
    # For each session number range, create or use existing respective branch, write to branch and then commit to branch
    for session_number_range in session_number_ranges:
        
        # Create or use existing respective branch
        branch = get_branch(session_number_range)

        logger.info(f"✍️ Started writing to branch {branch}")

        # Charging sessions having session number in the range
        filtered_df = df.filter(col("session_number_range") == session_number_range)

        # Write to branch
        filtered_df.write \
            .format("parquet") \
            .mode("append") \
            .option("path", f"s3a://{REPOSITORY}/{branch}/charging-events/charging_session_{session_number_range}") \
            .save()
        
        logger.info(f"✍️ Finished writing to branch {branch}")
        
        # Commit to branch
        logger.info(f"✅ Committed batch {session_number_range}")
        commit_to_branch(REPOSITORY, branch, f"Committing batch {session_number_range}")
        logger.info(f"✅ Finished committing batch {session_number_range}")
    
    return df

def ingest_charging_events_data(spark, schema):
    
    logger.info("Creating test DataFrame for S3AFileSystem test")
    
    # Create a test DataFrame
    test_schema = StructType([
        StructField("event_type", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("session_number", IntegerType(), True),
        StructField("station_id", StringType(), True),
        StructField("ev_id", StringType(), True),
        StructField("payload", StringType(), True)
    ])
    
    # Continue with the original Kafka streaming logic if needed
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
    blob_query.awaitTermination()

def process_batch(data_frame, batch_id):
    
    logger.info(f"Processing batch {batch_id}")
    
    write_to_branch_and_commit(data_frame)
    
    logger.info(f"Finished processing batch {batch_id}")