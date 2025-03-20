import os
from clickhouse.clickhouse_repository_base import ClickhouseRepositoryBase
from clickhouse_connect.driver.client import Client
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr, concat_ws

class DimStationsRepository(ClickhouseRepositoryBase):
    def __init__(self, clickhouse_conn: str):
        super().__init__(clickhouse_conn)

    def insert_stations(self, stations_df: DataFrame):
        try:
            if stations_df.isEmpty():
                self.logger.warning("No valid stations to load into ClickHouse")
                return

            stations_df = stations_df \
                .withColumnRenamed("_id", "id") \
                .withColumnRenamed("stationId", "station_name") \
                .withColumn("latitude", expr("location.coordinates[1]")) \
                .withColumn("longitude", expr("location.coordinates[0]")) \
                .withColumnRenamed("chargerType", "charger_type") \
                .withColumnRenamed("Cost", "cost") \
                .withColumnRenamed("distanceToCity", "distance_to_city") \
                .withColumnRenamed("operator", "operator") \
                .withColumnRenamed("chargingCapacity", "charging_capacity") \
                .withColumn("connector_type", concat_ws(",", col("connectorTypes"))) \
                .withColumnRenamed("installationYear", "installation_year") \
                .withColumnRenamed("renewableEnergySource", "renewable_energy_source") \
                .withColumnRenamed("maintenanceFrequency", "maintenance_frequency") \
                .withColumnRenamed("parkingSpots", "parking_spot") \
                .drop(col("availability")) \
                .drop(col("location")) \
                .drop(col("reviews")) \
                .drop(col("usageStats")) \
                .drop(col("connectorTypes"))
                
            stations_df.write \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("dbtable", "dim_station") \
                .option("user", self.connection.login) \
                .option("password", self.connection.password) \
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
                .option("batchsize", 10000) \
                .mode("append") \
                .save()
            
        except Exception as e:
            self.logger.error(f"Error inserting stations into ClickHouse: {e}")
            raise e
