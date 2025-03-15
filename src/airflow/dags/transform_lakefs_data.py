#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Spark application to transform data from LakeFS based on a specific commit ID.
This script is called by Airflow's SparkSubmitOperator and receives the commit_id as an argument.
"""

import argparse, logging, os, sys, lakefs_client
from pyspark.sql import SparkSession
from lakefs_client.client import LakeFSClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

LAKEFS_ENDPOINT = os.getenv("LAKEFS_ENDPOINT", "http://lakefs:8000")
LAKEFS_ACCESS_KEY = os.getenv("LAKEFS_ACCESS_KEY", "AKIAJBWUDLDFGJY36X3Q")
LAKEFS_SECRET_KEY = os.getenv("LAKEFS_SECRET_KEY", "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")

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Transform data from LakeFS')
    parser.add_argument('--commit_id', required=True, help='LakeFS commit ID to process')
    parser.add_argument('--repository', required=True, help='LakeFS repository name')
    parser.add_argument('--branch', required=True, help='LakeFS branch name')
    parser.add_argument('--lakefs_host', default=LAKEFS_ENDPOINT, help='LakeFS host URL')
    parser.add_argument('--lakefs_access_key', default=LAKEFS_ACCESS_KEY, help='LakeFS access key')
    parser.add_argument('--lakefs_secret_key', default=LAKEFS_SECRET_KEY, help='LakeFS secret key')
    return parser.parse_args()

def create_spark_session():
    """Create and configure a Spark session."""
    # Create a SparkSession
    spark = (SparkSession.builder
        .appName("LakeFS Data Transformer") # type: ignore
        .master("spark://spark-master:7077")
        .config("spark.driver.extraClassPath", "/opt/bitnami/spark/jars/*")
        .config("spark.executor.extraClassPath", "/opt/bitnami/spark/jars/*")
        # Add the Hadoop AWS JAR to the classpath
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Specify the exact paths to the required JARs
        .config("spark.jars", "/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar,/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar")
        # Add S3A configuration
        .config("spark.hadoop.fs.s3a.endpoint", LAKEFS_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", LAKEFS_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", LAKEFS_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )
    
    # Log Spark configuration for debugging
    logger.info("Spark session created with the following configurations:")
    logger.info(f"Spark version: {spark.version}")
    
    # Log S3A configuration to verify it's properly set
    try:
        s3a_impl = spark.conf.get("spark.hadoop.fs.s3a.impl")
        logger.info(f"S3A implementation: {s3a_impl}")
        logger.info(f"S3A endpoint: {spark.conf.get('spark.hadoop.fs.s3a.endpoint')}")
        logger.info(f"Path style access: {spark.conf.get('spark.hadoop.fs.s3a.path.style.access')}")
        logger.info(f"AWS credentials provider: {spark.conf.get('spark.hadoop.fs.s3a.aws.credentials.provider')}")
        
        # List available classes to verify S3AFileSystem is available
        logger.info("Checking if S3AFileSystem class is available in the classpath")
        try:
            from py4j.java_gateway import JavaClass
            s3a_class = spark._jvm.org.apache.hadoop.fs.s3a.S3AFileSystem
            logger.info(f"S3AFileSystem class found: {s3a_class}")
        except Exception as e:
            logger.error(f"S3AFileSystem class not found: {str(e)}")
            
        # List all available Hadoop filesystem implementations
        try:
            logger.info("Listing available Hadoop filesystem implementations:")
            hadoop_conf = spark._jsc.hadoopConfiguration()
            fs_impls = hadoop_conf.getValByRegex("fs\\..*\\.impl")
            for key in fs_impls.keySet().toArray():
                logger.info(f"  {key}: {fs_impls.get(key)}")
        except Exception as e:
            logger.error(f"Could not list filesystem implementations: {str(e)}")
            
    except Exception as e:
        logger.warning(f"Could not retrieve S3A configuration: {str(e)}")
    
    return spark

def get_lakefs_client(host, access_key, secret_key):
    """Create and return a LakeFS client."""
    configuration = lakefs_client.Configuration()
    configuration.host = host
    configuration.username = access_key
    configuration.password = secret_key
    return LakeFSClient(configuration)

def get_files_from_commit(repo_name, branch_name, commit_id, lakefs_client):
    try:
        # Get the commit details
        commits_api = lakefs_client.commits_api
        commit_details = commits_api.get_commit(repo_name, commit_id)
        
        # Log commit information
        logger.info(f"Processing merge commit: {commit_id}")
        logger.info(f"Parents: {commit_details.parents}")
        
        # Verify this is a merge commit (should have at least 2 parents)
        if len(commit_details.parents) < 2:
            error_msg = f"Expected a merge commit, but found only {len(commit_details.parents)} parents for commit {commit_id}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # The second parent is the tip of the feature branch that was merged
        feature_branch_tip = commit_details.parents[1]
        logger.info(f"Using feature branch tip: {feature_branch_tip}")
        
        # Get all files in the feature branch tip
        objects_api = lakefs_client.objects_api
        files = []
        
        # Use pagination to get all objects
        after = ""
        while True:
            objects = objects_api.list_objects(repo_name, feature_branch_tip, amount=1000, after=after)
            for obj in objects.results:
                # Only include parquet files
                if obj.path.endswith('.parquet'):
                    files.append(obj.path)
            if not objects.pagination.has_more:
                break
            after = objects.pagination.next_offset
        
        logger.info(f"Found {len(files)} parquet files in the feature branch that was merged")
        
        return files
    
    except Exception as e:
        logger.error(f"Error getting files from merge commit: {str(e)}")
        raise

def get_lakefs_path(repository: str, commit_id: str, path: str):
    """
    Construct the proper path to access LakeFS data via S3A protocol.
    
    LakeFS uses the format: s3a://<repository>/<ref>/<path>
    where <ref> can be a branch name, commit ID, or tag.
    """
    logger.info(f"Constructing LakeFS path for repo: {repository}, commit: {commit_id}, path: {path}")
    lakefs_path = f"s3a://{repository}/{commit_id}/{path}"
    logger.info(f"Using LakeFS path: {lakefs_path}")
    return lakefs_path

def main():
    """Main entry point for the Spark application."""
    # Check for Java installation
    java_home = os.getenv("JAVA_HOME")
    if java_home:
        logger.info(f"Using JAVA_HOME from environment: {java_home}")
    else:
        logger.warning("JAVA_HOME environment variable is not set!")
        sys.exit(1)
    
    # Parse command line arguments
    args = parse_arguments()
    
    # Log the arguments
    logger.info(f"Starting LakeFS data transformation job")
    logger.info(f"Processing commit: {args.commit_id}")
    logger.info(f"Repository: {args.repository}")
    logger.info(f"Branch: {args.branch}")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Create LakeFS client
        lakefs_client = get_lakefs_client(
            args.lakefs_host, 
            args.lakefs_access_key, 
            args.lakefs_secret_key
        )
        
        # Get the list of files from the commit
        files = get_files_from_commit(
            args.repository, 
            args.branch, 
            args.commit_id, 
            lakefs_client
        )
        
        if not files:
            logger.warning(f"No parquet files found in commit {args.commit_id}")
            spark.stop()
            return
        
        logger.info(f"Processing {len(files)} parquet files from commit {args.commit_id}")
        
        # Process each file
        for file_path in files:
            try:
                # Construct the LakeFS path for the file
                lakefs_path = get_lakefs_path(args.repository, args.commit_id, file_path)
                
                logger.info(f"Reading data from LakeFS path: {lakefs_path}")
                
                # Read the parquet file
                try:
                    logger.info(f"Attempting to read parquet file from: {lakefs_path}")
                    # Try to read the file
                    df = spark.read.parquet(lakefs_path)
                    logger.info(f"Successfully loaded parquet data from {lakefs_path}")
                    
                    # Print the schema and a sample of the data
                    logger.info(f"Data Schema for {file_path}:")
                    df.printSchema()
                    
                    logger.info(f"Data Sample for {file_path}:")
                    df.show(5, truncate=False)
                    
                    # Get row count
                    row_count = df.count()
                    logger.info(f"Total rows in {file_path}: {row_count}")
                    
                    # TODO: Add your transformation logic here
                    # transformed_df = ...
                    
                    # TODO: Write the transformed data to the output location
                    # transformed_df.write.mode("overwrite").parquet(output_path)
                    
                except Exception as e:
                    logger.error(f"Error reading parquet data from {lakefs_path}: {str(e)}")
                    continue
            
            except Exception as e:
                logger.error(f"Error processing file {file_path}: {str(e)}")
                continue
        
        logger.info("Transformation completed successfully!")
        
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        spark.stop()
        sys.exit(1)
    
    # Stop the Spark session
    spark.stop()
    
if __name__ == "__main__":
    main() 