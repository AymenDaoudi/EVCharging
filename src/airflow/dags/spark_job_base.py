import argparse
import logging, os, sys
from pyspark.sql import SparkSession

class SparkJobBase:
    def __init__(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__class__.__name__)
    
    def check_java_installation(self):
        """Check for Java installation and ensure JAVA_HOME is set."""
        java_home = os.getenv("JAVA_HOME")
        if java_home:
            self.logger.info(f"Using JAVA_HOME from environment: {java_home}")
        else:
            self.logger.warning("JAVA_HOME environment variable is not set!")
            sys.exit(1)
    
    def create_spark_session(self, app_name: str, configs: dict[str, str]):
        """Create and configure Spark session with MongoDB connector."""
        
        spark_session_builder = SparkSession.builder.appName(app_name) # type: ignore
        
        for key, value in configs.items():
            spark_session_builder = spark_session_builder.config(key, value)
        
        spark = spark_session_builder.getOrCreate()
        
        self.logger.info(f"Spark session created with spark version: {spark.version}")
        return spark