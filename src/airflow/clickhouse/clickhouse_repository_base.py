from datetime import datetime
from clickhouse_connect.driver.client import Client
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
import logging
from logging import Logger
from clickhouse.clickhouse_config import ClickhouseConfig

class ClickhouseRepositoryBase:
    def __init__(self, clickhouse_conn: str):
        self.__clickhouse_config = ClickhouseConfig(clickhouse_conn)
        # Configure logging
        logging.basicConfig(level=logging.INFO)
        self.__logger = logging.getLogger(self.__class__.__name__)
        self.__jdbc_url = f"jdbc:clickhouse://{self.connection.host}:{self.connection.port}/{self.connection.schema}"
        
    @property
    def client(self) -> Client:
        return self.__clickhouse_config.client
    
    @property
    def connection(self):
        return self.__clickhouse_config.connection
    
    @property
    def logger(self) -> Logger:
        return self.__logger
    
    @property
    def jdbc_url(self):
        return self.__jdbc_url
    
    def generate_time_id(self, dt: datetime) -> int:
        # Format: YYYYMMDDHH
        return int(dt.strftime("%Y%m%d%H"))
    
    def insert_time_dimension(self, dt: datetime) -> int:
        time_id = self.generate_time_id(dt)
        
        # Check if the time_id already exists
        result = self.client.query(f"SELECT 1 FROM dim_time WHERE time_id = {time_id}")
        if result.result_rows:
            self.__logger.info(f"Time dimension record for time_id {time_id} already exists")
            return time_id
        
        # Insert the new time dimension record
        is_weekend = 1 if dt.weekday() >= 5 else 0  # 5 and 6 are Saturday and Sunday
        
        # Simple holiday check (can be expanded with a proper holiday calendar)
        is_holiday = 0
        
        self.client.command(
            """
            INSERT INTO dim_time (time_id, hour, day, month, year, day_of_week, is_weekend, is_holiday)
            VALUES (%(time_id)s, %(hour)s, %(day)s, %(month)s, %(year)s, %(day_of_week)s, %(is_weekend)s, %(is_holiday)s)
            """,
            parameters={
                "time_id": time_id,
                "hour": dt.hour,
                "day": dt.day,
                "month": dt.month,
                "year": dt.year,
                "day_of_week": dt.weekday(),
                "is_weekend": is_weekend,
                "is_holiday": is_holiday
            }
        )
        
        self.__logger.info(f"Inserted time dimension record for time_id {time_id}")
        return time_id