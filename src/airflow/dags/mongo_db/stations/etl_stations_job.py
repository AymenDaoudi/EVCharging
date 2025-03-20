#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Spark application to extract data from MongoDB's ElectricVehicles collection.
This script is called by Airflow's SparkSubmitOperator.
"""

import logging, sys
from clickhouse.dim_stations_repository import DimStationsRepository
from mongodb.mongo_spark_repository import MongoSparkRepository
from dags.spark_job_base import SparkJobBase

class ExtractStationsJob(SparkJobBase):
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(__class__.__name__)
        self.mongo_spark_repository = MongoSparkRepository('mongodb_conn', 'charging_stations_collection')
        self.dim_stations_repository = DimStationsRepository('clickhouse_conn')

    def main(self):
        """Main entry point for the Spark application."""
        self.check_java_installation()
        
        # Log the start of the job
        self.logger.info("Starting MongoDB data extraction job")
        
        # Create Spark session
        spark = self.create_spark_session(
            app_name='Extract Stations Job',
            configs={
                'spark.mongodb.input.uri': self.mongo_spark_repository.mongo_uri,
                'spark.mongodb.input.database': 'Db',
                'spark.mongodb.input.collection': 'ChargingStations'
            }
        )
        
        try:
            # Extract data from MongoDB
            self.logger.info("Extracting stations from MongoDB...")
            df = self.mongo_spark_repository.extract_data_from_mongodb(spark)
            self.logger.info("Data extraction completed successfully!")
            
            self.logger.info("Loading data to ClickHouse...")
            self.dim_stations_repository.insert_stations(df)
            self.logger.info("Successfully loaded data to ClickHouse !")
            
        except Exception as e:
            self.logger.error(f"Error in main execution: {str(e)}")
            sys.exit(1)
        finally:
            spark.stop()

if __name__ == "__main__":
    job = ExtractStationsJob()
    job.main()
