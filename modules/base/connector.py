from abc import ABCMeta

from sqlalchemy import create_engine
from sqlalchemy import text
from decouple import config

class DatabaseConnector(metaclass=ABCMeta):
    def __init__(self):
        engine_string = \
            DatabaseConnector.get_engine_string(
                    username=config('DB_USER'),
                    password=config('DB_PASSWORD'), 
                    host=config('DB_HOST'), 
                    port=config('DB_PORT'), 
                    database_name=config('DB_NAME')
                )
        self.__engine = create_engine(engine_string)

    def get_engine(self):
        return self.__engine

    def execute_returned_sql_transaction(
        self, 
        query_function, 
        *args, 
        **kwargs
    ):
        def wrapper(*args, **kwargs):
            query = text(query_function(*args, **kwargs))
            results = self.__engine.execute(query)
            return results
        return wrapper

    @staticmethod
    def get_engine_string(
        username,
        password,
        host,
        port,
        database_name
    ) -> str:
        return f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database_name}'