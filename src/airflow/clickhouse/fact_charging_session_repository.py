import os
from clickhouse.clickhouse_repository_base import ClickhouseRepositoryBase
from clickhouse_connect.driver.client import Client
from pyspark.sql import DataFrame, SparkSession
from datetime import datetime
from pyspark.sql.functions import lit, col, udf
from pyspark.sql.types import IntegerType
from functional import seq
# ClickHouse connection parameters
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "admin")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "admin")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "ev_charging")

class FactChargingSessionRepository(ClickhouseRepositoryBase):
    def __init__(self, clickhouse_client: Client):
        super().__init__(clickhouse_client)
        
    def insert_fact_sessions_dataframe(self, spark, sessions_df: DataFrame):
        try:
            if sessions_df.isEmpty():
                self.__logger.warning("No valid sessions to load into ClickHouse")
                return

            # Define local function for time ID generation
            # we cannot use the generate_time_id method from the base class because it is reached with the keyword self
            # and that is not serializable by Spark
            def generate_time_id(dt):
                if dt is None:
                    return None
                # Convert string to datetime if needed
                if isinstance(dt, str):
                    # Handle ISO format with microseconds
                    dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))
                return int(dt.strftime("%Y%m%d%H"))

            # Register the UDF using the local function
            generate_time_id_udf = udf(generate_time_id, IntegerType())

            # Add time dimension IDs for both start and stop times
            sessions_df = sessions_df \
                .withColumn("start_time_id", generate_time_id_udf(col("start_time"))) \
                .withColumn("end_time_id", generate_time_id_udf(col("stop_time"))) \
                .drop(col("start_time")) \
                .drop(col("stop_time"))
                
            # Get the total count for logging
            total_sessions = sessions_df.count()
            self.logger.info(f"Preparing to write {total_sessions} sessions to ClickHouse")
            
            # insert time dimension
            unique_time_ids = sessions_df.select("start_time_id", "end_time_id") \
                .distinct() \
                .collect()
            
            seq(unique_time_ids) \
                .map(lambda row: [row.start_time_id, row.end_time_id]) \
                .flat_map(lambda time_ids: time_ids) \
                .map(lambda time_id: self.insert_time_dimension(datetime.strptime(str(time_id), "%Y%m%d%H"))) \
                .to_list()
            
            # Now write the sessions data
            jdbc_url = f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DATABASE}"
            
            sessions_df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", "fact_charging_sessions") \
                .option("user", CLICKHOUSE_USER) \
                .option("password", CLICKHOUSE_PASSWORD) \
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
                .option("batchsize", 10000) \
                .mode("append") \
                .save()
            
            self.logger.info(f"Successfully loaded {total_sessions} sessions into ClickHouse using JDBC")
            
        except Exception as e:
            self.logger.error(f"Error inserting fact charging sessions: {e}")
            raise e
