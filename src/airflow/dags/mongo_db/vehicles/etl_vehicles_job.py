#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Spark application to extract data from MongoDB's ElectricVehicles collection.
This script is called by Airflow's SparkSubmitOperator.
"""

import logging, sys
from clickhouse.dim_vehicles_repository import DimVehiclesRepository
from dags.spark_job_base import SparkJobBase
from mongodb.mongo_spark_repository import MongoSparkRepository

class ExtractVehiclesJob(SparkJobBase):
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(__class__.__name__)
        self.mongo_spark_repository = MongoSparkRepository(
            'mongodb_conn', 
            'electric_vehicles_collection')
        self.dim_vehicles_repository = DimVehiclesRepository('clickhouse_conn')

    def main(self):
        """Main entry point for the Spark application."""
        
        self.check_java_installation()
        
        # Log the start of the job
        self.logger.info("Starting MongoDB data extraction job")
        
        # Create Spark session
        spark = self.create_spark_session(
            app_name='Extract Vehicles Job',
            configs={
                'spark.mongodb.input.uri': self.mongo_spark_repository.mongo_uri,
                'spark.mongodb.input.database': 'Db',
                'spark.mongodb.input.collection': 'ElectricVehicles'
            }
        )
        
        try:
            # Extract data from MongoDB
            self.logger.info("Extracting vehicles from MongoDB...")
            vehicles_df = self.mongo_spark_repository.extract_data_from_mongodb(spark)
            self.logger.info("Data extraction completed successfully!")
            
            # Load data to ClickHouse
            self.logger.info("Loading data to ClickHouse...")
            self.dim_vehicles_repository.insert_vehicles(vehicles_df)
            self.logger.info("Successfully loaded data to ClickHouse !")
            
        except Exception as e:
            self.logger.error(f"Error in main execution: {str(e)}")
            sys.exit(1)
        finally:
            spark.stop()

if __name__ == "__main__":
    job = ExtractVehiclesJob()
    job.main()