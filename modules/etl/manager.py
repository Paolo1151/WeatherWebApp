import os
import sys
from abc import ABCMeta
sys.path.append(os.path.join('..', '..'))

import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import config
from modules.base.connector import DatabaseConnector

class Transformer():
    def __init__(self, name, statement):
        self.__name = name
        self.__statement = statement

    def get_statement(self):
        return self.__statement

'''
Expansions
'''

class WeatherDatabaseManager(DatabaseConnector):
    def __init__(self):
        self.create_transforms()
        self.create_date_table()

    ## Database Deployment
    def create_transforms(self):
        self.__transformers = []
        for file in filter(lambda x: x.startswith('Transform') ,os.listdir(os.path.join(config.MODULES_PATH, 'etl', 'sql'))):
            with open(os.path.join(os.path.join(config.MODULES_PATH, 'etl', 'sql', file)), 'r') as stmt:
                tran = Transformer(file.removesuffix('.sql'), stmt.read())
                self.__transformers.append(tran)
         
    @self.execute_returned_sql_transaction
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
            rows = df.to_sql(f'{name}', self.get_engine(), schema='public', if_exists='append', index=False)

    def transform_and_load(self):
        for transformer in self.__transformers:
            WeatherDatabaseManager.execute_transform_statement(transformer)
            
    @staticmethod
    @self.execute_returned_sql_transaction
    def execute_transform_statement(transformer):
        return transformer.get_statement()
