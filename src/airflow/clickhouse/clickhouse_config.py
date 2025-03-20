from airflow.hooks.base import BaseHook
import clickhouse_connect

class ClickhouseConfig:
    def __init__(self, clickhouse_conn: str):
        self.__connection = BaseHook.get_connection(clickhouse_conn)
    
        assert self.__connection.host is not None
        assert self.__connection.port is not None
        assert self.__connection.login is not None
        assert self.__connection.password is not None
        assert self.__connection.schema is not None
        
        # Initialize ClickHouse client using clickhouse_connect
        self.__client = clickhouse_connect.get_client(
            host=self.__connection.host,
            port=self.__connection.port,
            username=self.__connection.login,
            password=self.__connection.password,
            database=self.__connection.schema
        )
        
    @property
    def client(self):
        return self.__client
    
    @property
    def connection(self):
        return self.__connection