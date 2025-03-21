from pyspark.sql.functions import count, col, sum, when, coalesce, max, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

def transform_data(spark, df):
    """Transform the data."""
    
    # Since payload is already a Map type, we can access its fields directly
    df_with_payload = df.withColumn(
        "start_time",
        when(col("event_type") == "start", col("payload.timestamp")).otherwise(None)
    ).withColumn(
        "stop_time",
        when(col("event_type") == "stop", col("payload.timestamp")).otherwise(None)
    ).withColumn(
        "energy_delivered",
        coalesce(col("payload.energy_delivered"), lit(None))
    ).withColumn(
        "duration_minutes",
        coalesce(col("payload.duration_minutes"), lit(None))
    )
    
    # Group by session_id and count the number of events per session
    session_groupings = df_with_payload.groupBy("session_id", "station_id", "vehicle_id").agg(
        count("*").alias("sessions_count"),
        # Include other relevant session information
        max(col("start_time")).alias("start_time"),
        max(col("stop_time")).alias("stop_time"),
        max(col("energy_delivered")).alias("energy_delivered"),
        max(col("duration_minutes")).alias("duration_minutes"),
        # Count start events
        sum(when(col("event_type") == "start", 1).otherwise(0)).alias("start_events"),
        # Count completion events
        sum(when(col("event_type") == "stop", 1).otherwise(0)).alias("completion_events")
    )
    
    # Filter sessions with exactly one start event and at least one completion event
    valid_sessions = session_groupings.filter(
        (col("start_events") == 1) & (col("completion_events") >= 1)
    )
    
    invalid_sessions = session_groupings.filter(
        (col("start_events")!= 1) | (col("completion_events") < 1)
    )
    
    # Count of valid vs invalid sessions
    total_sessions = session_groupings.count()
    valid_session_count = valid_sessions.count()
    
    print(f"Total sessions: {total_sessions}")
    print(f"Valid sessions: {valid_session_count}")
    print(f"Invalid sessions: {total_sessions - valid_session_count}")
    
    # Remove start_events column from valid_sessions
    valid_sessions = valid_sessions \
        .drop("sessions_count") \
        .drop("start_events") \
        .drop("completion_events")
        
    invalid_sessions = invalid_sessions \
        .drop("sessions_count") \
        .drop("start_events") \
        .drop("completion_events")
        
    return valid_sessions, invalid_sessions