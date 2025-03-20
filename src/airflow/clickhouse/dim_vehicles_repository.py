import os
from clickhouse.clickhouse_repository_base import ClickhouseRepositoryBase
from clickhouse_connect.driver.client import Client
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr

class DimVehiclesRepository(ClickhouseRepositoryBase):
    def __init__(self, clickhouse_conn: str):
        super().__init__(clickhouse_conn)

    def insert_vehicles(self, vehicles_df: DataFrame):
        try:
            if vehicles_df.isEmpty():
                self.logger.warning("No valid vehicles to load into ClickHouse")
                return
            
            # Rename columns and cast types to match ClickHouse schema
            vehicles_df = vehicles_df \
                .withColumnRenamed("Name", "model") \
                .withColumnRenamed("Acceleration_0_100", "acceleration_0_100") \
                .withColumnRenamed("Fast_charge", "fast_charge") \
                .withColumnRenamed("Battery_capacity", "battery_capacity") \
                .withColumnRenamed("_id", "id") \
                .withColumn("efficiency", col("efficiency").cast("integer")) \
                .withColumn("fast_charge", col("fast_charge").cast("integer")) \
                .withColumn("range", col("range").cast("integer")) \
                .withColumn("Top_speed", col("top_speed").cast("integer")) \
                .withColumn("acceleration_0_100", col("acceleration_0_100").cast("integer")) \
                .withColumn("battery_capacity", col("battery_capacity").cast("float")) \
                .drop(col("Price"))
            
            vehicles_df.write \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("dbtable", "dim_vehicle") \
                .option("user", self.connection.login) \
                .option("password", self.connection.password) \
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
                .option("batchsize", 10000) \
                .mode("append") \
                .save()

        except Exception as e:
            self.logger.error(f"Error inserting vehicles into ClickHouse: {e}")
            raise e
