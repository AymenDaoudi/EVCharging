#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Spark application to transform data from LakeFS based on a specific commit ID.
This script is called by Airflow's SparkSubmitOperator and receives the commit_id as an argument.
"""

import argparse, logging, os, sys
import lakefs_client as lakefs
from clickhouse_connect.driver.client import Client
from pyspark.sql import SparkSession
from lakefs_client.client import LakeFSClient
from clickhouse.fact_charging_session_repository import FactChargingSessionRepository
from transform_data import transform_data
import clickhouse_connect
from airflow.hooks.base import BaseHook

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REPOSITORY = os.getenv("LAKEFS_REPOSITORY", "charging-data")
BRANCH = os.getenv("LAKEFS_BRANCH", "main")
LAKEFS_ENDPOINT = os.getenv("LAKEFS_ENDPOINT", "http://lakefs:8000")
LAKEFS_ACCESS_KEY = os.getenv("LAKEFS_ACCESS_KEY", "AKIAJBWUDLDFGJY36X3Q")
LAKEFS_SECRET_KEY = os.getenv("LAKEFS_SECRET_KEY", "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")
# ClickHouse connection parameters
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "admin")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "admin")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "ev_charging")

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Transform data from LakeFS')
    parser.add_argument('--commit_id', required=True, help='LakeFS commit ID to process')
    parser.add_argument('--repository', required=True, help='LakeFS repository name')
    parser.add_argument('--branch', required=True, help='LakeFS branch name')
    return parser.parse_args()

def check_java_installation():
    """Check for Java installation and ensure JAVA_HOME is set."""
    java_home = os.getenv("JAVA_HOME")
    if java_home:
        logger.info(f"Using JAVA_HOME from environment: {java_home}")
    else:
        logger.warning("JAVA_HOME environment variable is not set!")
        sys.exit(1)

def create_spark_session():

    spark = (SparkSession.builder
        .appName("LakeFS Data Transformer") # type: ignore
        .getOrCreate()
    )
    
    logger.info(f"Spark session created with spark version: {spark.version}")
    
    return spark

def get_lakefs_client(host, access_key, secret_key):

    configuration = lakefs.Configuration()
    configuration.host = host
    configuration.username = access_key
    configuration.password = secret_key
    return LakeFSClient(configuration)

def get_files_from_merge_commit(
    repo_name: str,
    commit_id: str,
    lakefs_client: LakeFSClient
) -> list[str]:
    
    try:
        # Get the commit details
        commits_api = lakefs_client.commits_api # type: ignore
        commit_details = commits_api.get_commit(repo_name, commit_id)
        
        # Log commit information
        logger.info(f"Processing merge commit: {commit_id}")
        
        # Verify this is a merge commit (should have at least 2 parents)
        if len(commit_details.parents) < 2:
            error_msg = f"Expected a merge commit, but found only {len(commit_details.parents)} parents for commit {commit_id}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Get the second parent is the tip of the feature branch that was merged
        feature_branch_tip = commit_details.parents[1]
        logger.info(f"Using feature branch tip: {feature_branch_tip}")
        
        # Get all files in the feature branch tip
        objects_api = lakefs_client.objects_api # type: ignore
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
    
    LakeFS uses the format: s3a://<repository>/<commit_id>/<path>
    where <commit_id> is the commit ID of the merge commit.
    """
    logger.info(f"Constructing LakeFS path for repo: {repository}, commit: {commit_id}, path: {path}")
    lakefs_path = f"s3a://{repository}/{commit_id}/{path}"
    logger.info(f"Using LakeFS path: {lakefs_path}")
    
    return lakefs_path

def get_data_from_committed_file(
    spark: SparkSession,
    args: argparse.Namespace
):
    try:
        # Create LakeFS client
        lakefs_client = get_lakefs_client(
            LAKEFS_ENDPOINT, 
            LAKEFS_ACCESS_KEY, 
            LAKEFS_SECRET_KEY
        )
        
        # Get the list of files from the commit
        files = get_files_from_merge_commit(
            args.repository, 
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
                    
                    return df
                    
                except Exception as e:
                    logger.error(f"Error reading parquet data from {lakefs_path}: {str(e)}")
                    continue
            
            except Exception as e:
                logger.error(f"Error processing file {file_path}: {str(e)}")
                continue
        
        logger.info("Transformation and loading completed successfully!")
        
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        spark.stop()
        sys.exit(1)
    
def main():
    """Main entry point for the Spark application."""
    
    check_java_installation()
    
    # Parse command line arguments
    args = parse_arguments()
    
    # Log the arguments
    logger.info(f"Starting LakeFS data transformation job")
    logger.info(f"Repository: {args.repository}")
    logger.info(f"Processing commit: {args.commit_id}")
    
    # Create Spark session
    spark = create_spark_session()
    
    loaded_df = get_data_from_committed_file(spark, args)
    
    valid_sessions, invalid_sessions = transform_data(spark, loaded_df)

    # Get ClickHouse connection from Airflow
    clickhouse_conn = BaseHook.get_connection('clickhouse_conn')
    
    assert clickhouse_conn.host is not None
    assert clickhouse_conn.port is not None
    assert clickhouse_conn.login is not None
    assert clickhouse_conn.password is not None
    assert clickhouse_conn.schema is not None
    
    # Initialize ClickHouse client using Airflow connection
    clickhouse_client = clickhouse_connect.get_client(
        host=clickhouse_conn.host,
        port=clickhouse_conn.port,
        username=clickhouse_conn.login,
        password=clickhouse_conn.password,
        database=clickhouse_conn.schema
    )
    
    logger.info("Loading valid sessions to ClickHouse...")
    
    fact_charging_session_repository = FactChargingSessionRepository(clickhouse_client)
    fact_charging_session_repository.insert_fact_sessions_dataframe(spark, valid_sessions)

    logger.info("Successfully loaded valid sessions to ClickHouse !")
    
    # Stop the Spark session
    spark.stop()
    
if __name__ == "__main__":
    main() 