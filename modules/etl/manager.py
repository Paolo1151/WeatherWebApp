import os
import sys
from abc import ABCMeta
sys.path.append(os.path.join('..', '..'))

import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import config

def execute_returned_sql_transaction(query_function, *args, **kwargs):
    def wrapper(*args, **kwargs):
        with psycopg2.connect(
            host=config('DB_HOST'),
            database=config('DB_NAME'),
            user=config('DB_USER'),
            password=config('DB_PASSWORD')
        ) as conn:
            cur = conn.cursor()
            query = query_function(*args, **kwargs)
            cur.execute(query)
            res = cur.fetchall()
            cur.close()
        return res
    return wrapper


class DatabaseConnector(metaclass=ABCMeta):
    def __init__(self):
        pass

    @staticmethod
    def get_engine_string(
        username,
        password,
        host,
        port,
        database_name
    ) -> str:
        return f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database_name}'

class Transformer():
    def __init__(self, name, statement):
        self.__name = name
        self.__statement = statement

    @execute_returned_sql_transaction
    def execute(self):
        return self.__statement

'''
Expansions
'''

class WeatherDatabaseManager(DatabaseConnector):
    def __init__(self):
        engine_string = \
            DatabaseConnector.get_engine_string(
                    username=config('DB_USER'),
                    password=config('DB_PASS'), 
                    host=config('DB_HOST'), 
                    port=config('DB_PORT'), 
                    database_name=config('DB_NAME')
                )
        self.__engine = create_engine(engine_string)
        self.create_transforms()
        self.create_date_table()

    ## Database Deployment
    def create_transforms(self):
        self.__transformers = []
        for file in filter(lambda x: x.startswith('Transform') ,os.listdir(os.path.join(config.MODULES_PATH, 'etl', 'sql'))):
            with open(os.path.join(os.path.join(config.MODULES_PATH, 'etl', 'sql', file)), 'r') as stmt:
                tran = Transformer(file.removesuffix('.sql'), stmt.read())
                self.__transformers.append(tran)
         
    @execute_returned_sql_transaction
    def create_date_table(self):
        with open(os.path.join(os.path.join(config.MODULES_PATH, 'etl', 'sql', 'CreateDate.sql')), 'r') as stmt:
            return stmt.read()

    ## ETL
    def perform_etl(self):
        self.extract_from_csv()
        self.transform_and_load()

    ## Extraction of Data into source from csvs
    def extract_from_csv(self):
        # Dynamically Get the Data from the CSV
        for file in ['province_detail.csv', 'station_detail.csv', 'climate_data.csv']:
            df = pd.read_csv(os.path.join(config.DATA_PATH, file))
            name = file.strip.split('_')[0]
            df.columns = [col.lower() for col in df.columns]
            rows = df.to_sql(f'{name}', self.__engine, schema='public', if_exists='append', index=False)

    ## Transformation of Data
    ## Loading to Data Warehouse
    def transform_and_load(self):
        # Execute all Transforms
        for transformer in self.__transformers:
            transformer.execute()