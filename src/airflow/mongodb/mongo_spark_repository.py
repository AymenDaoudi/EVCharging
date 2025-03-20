import logging
from pyspark.sql import SparkSession
from airflow.hooks.base import BaseHook

class MongoSparkRepository:
    def __init__(self, mongodb_connection: str, collection_name: str):
        logging.basicConfig(level=logging.INFO)
        self.__logger = logging.getLogger(__class__.__name__)
        self.__mongo_uri = self.__get_get_mongodbb_connection(mongodb_connection, collection_name)
    
    @property
    def mongo_uri(self):
        return self.__mongo_uri
    
    def __get_get_mongodbb_connection(self, mongodb_connection: str, collection_name: str):
        try:
            # Get MongoDB connection from Airflow
            mongo_conn = BaseHook.get_connection(mongodb_connection)
            
            # Extract connection details
            host = mongo_conn.host
            port = mongo_conn.port
            login = mongo_conn.login
            password = mongo_conn.password
            extra = mongo_conn.extra
            
            # Parse the extra JSON to get database and collection
            import json
            extra_dict = json.loads(extra)
            database = extra_dict.get('database')
            collection = extra_dict.get(collection_name)
            
            # Construct MongoDB URI with authentication
            mongo_uri = f"mongodb://{login}:{password}@{host}:{port}/{database}.{collection}"
            
            self.__logger.info(f"Using MongoDB connection: {mongo_uri}")
            return mongo_uri
            
        except Exception as e:
            self.__logger.error(f"Error getting MongoDB connection: {str(e)}")
            raise
        
    def extract_data_from_mongodb(self, spark: SparkSession):
        """Extract data from MongoDB's ElectricVehicles collection."""
        try:
            # Read data from MongoDB
            self.__logger.info("Reading data from MongoDB collection...")
            df = spark.read.format("mongo").load()
            
            # Log the count of records
            count = df.count()
            self.__logger.info(f"Successfully extracted {count} records from MongoDB")
            
            # Show sample data
            self.__logger.info("Sample data from MongoDB:")
            df.show(5, truncate=False)
            
            return df
            
        except Exception as e:
            self.__logger.error(f"Error extracting data from MongoDB: {str(e)}")
            raise