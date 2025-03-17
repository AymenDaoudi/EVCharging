import argparse
import sys
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# LakeFS configuration
LAKEFS_ENDPOINT = os.getenv("LAKEFS_ENDPOINT", "http://lakefs:8000")
LAKEFS_ACCESS_KEY = os.getenv("LAKEFS_ACCESS_KEY", "AKIAJBWUDLDFGJY36X3Q")
LAKEFS_SECRET_KEY = os.getenv("LAKEFS_SECRET_KEY", "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")


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Transform data from LakeFS')
    parser.add_argument('--commit_id', help='LakeFS commit ID to process')
    parser.add_argument('--repository', required=True, help='LakeFS repository name')
    parser.add_argument('--branch', required=True, help='LakeFS branch name')
    parser.add_argument('--lakefs_host', default=LAKEFS_ENDPOINT, help='LakeFS host URL')
    parser.add_argument('--lakefs_access_key', default=LAKEFS_ACCESS_KEY, help='LakeFS access key')
    parser.add_argument('--lakefs_secret_key', default=LAKEFS_SECRET_KEY, help='LakeFS secret key')
    return parser.parse_args()

def create_spark_session():
    """Create and configure a Spark session with S3A support."""
    # Create a SparkSession
    spark = (SparkSession.builder
        .appName("ExampleSparkJob") # type: ignore
        # .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.hadoop:hadoop-common:3.3.4,org.apache.hadoop:hadoop-client:3.3.4")
        # # Add the Hadoop AWS JAR to the classpath
        # .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # # Add S3A configuration
        # .config("spark.hadoop.fs.s3a.endpoint", LAKEFS_ENDPOINT)
        # .config("spark.hadoop.fs.s3a.access.key", LAKEFS_ACCESS_KEY)
        # .config("spark.hadoop.fs.s3a.secret.key", LAKEFS_SECRET_KEY)
        # .config("spark.hadoop.fs.s3a.path.style.access", "true")
        # .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )
    
    # Log Spark configuration for debugging
    logger.info("Spark session created with the following configurations:")
    logger.info(f"Spark version: {spark.version}")
    
    return spark

def test_s3a_file_system(spark, repository):
    """
    Test if we can read the test file created by the streaming processor.
    This helps diagnose S3AFileSystem issues.
    """
    logger.info("=== TESTING S3A FILE SYSTEM FUNCTIONALITY ===")
    
    # Use the same path as in the streaming processor
    test_file_path = f"s3a://{repository}/main/test-data/test_file.parquet"
    
    try:
        logger.info(f"Attempting to read test file from: {test_file_path}")
        
        # Try to read the file
        test_df = spark.read.format("parquet").load(test_file_path)
        
        logger.info("Successfully read test file!")
        logger.info("Test DataFrame contents:")
        test_df.show()
        
        # Print Spark configuration for debugging
        logger.info("Current Spark configuration:")
        for conf in spark.sparkContext.getConf().getAll():
            logger.info(f"  {conf[0]}: {conf[1]}")
            
        return True
        
    except Exception as e:
        logger.error(f"Error reading test file: {str(e)}")
        logger.exception("Full exception details:")
        
        # Print Spark configuration for debugging
        logger.info("Current Spark configuration:")
        for conf in spark.sparkContext.getConf().getAll():
            logger.info(f"  {conf[0]}: {conf[1]}")
            
        return False

def run_example_job():
    """
    A simple example Spark job that:
    1. Creates a small dataframe
    2. Performs some transformations
    3. Shows the results
    4. Tests S3A file system functionality
    """
    
    args = parse_arguments()
    
    # Initialize Spark session with S3A support
    spark = create_spark_session()
    
    # Log the Spark version
    logger.info(f"Spark version: {spark.version}")
    
    s3a_test_result = test_s3a_file_system(spark, args.repository)
        
    if not s3a_test_result:
        logger.error("S3A file system test failed. Aborting job.")
        spark.stop()
        sys.exit(1)
    
    print("Job completed successfully!")
    
    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    run_example_job() 