import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp

def run_example_job():
    """
    A simple example Spark job that:
    1. Creates a small dataframe
    2. Performs some transformations
    3. Shows the results
    """
    # Initialize Spark session
    spark = (SparkSession.builder
        .appName("ExampleSparkJob") # type: ignore
        .getOrCreate())
    
    # Log the Spark version
    print(f"Spark version: {spark.version}")
    
    # Create a simple dataframe with some EV charging data
    data = [
        ("station_001", "ev_123", 45.5, "2023-01-01 10:00:00"),
        ("station_002", "ev_456", 30.2, "2023-01-01 11:30:00"),
        ("station_001", "ev_789", 22.7, "2023-01-01 14:15:00"),
        ("station_003", "ev_123", 50.0, "2023-01-02 09:45:00"),
        ("station_002", "ev_456", 35.8, "2023-01-02 16:20:00")
    ]
    
    columns = ["station_id", "ev_id", "energy_consumed_kwh", "timestamp"]
    df = spark.createDataFrame(data, columns)
    
    # Show the original dataframe
    print("Original DataFrame:")
    df.show()
    
    # Perform some transformations
    transformed_df = df \
        .withColumn("processing_time", current_timestamp()) \
        .withColumn("cost_usd", col("energy_consumed_kwh") * lit(0.15)) \
        .withColumn("data_source", lit("example_job"))
    
    # Show the transformed dataframe
    print("Transformed DataFrame:")
    transformed_df.show()
    
    # Calculate some aggregations
    print("Aggregation by Station:")
    df.groupBy("station_id") \
        .sum("energy_consumed_kwh") \
        .withColumnRenamed("sum(energy_consumed_kwh)", "total_energy_kwh") \
        .orderBy(col("total_energy_kwh").desc()) \
        .show()
    
    print("Job completed successfully!")
    
    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    run_example_job() 