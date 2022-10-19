import os
import sys
import warnings
warnings.filterwarnings('ignore')
sys.path.append(os.path.join('..', '..'))

import sqlalchemy
import pandas as pd

import config

# Data Source: https://www.kaggle.com/datasets/greegtitan/indonesia-climate?select=climate_data.csv
class WeatherDataTransformer():
    def __init__(self, engine):
        # Read Transformer Script
        with open(os.path.join(os.path.dirname(__file__), 'resources', 'transform_script.sql'), 'r') as ts:
            self.__script = ts.read()

        # Create Database Engine
        self.__dbEngine = engine

    def run(self, station_id=None):
        # Extract and Transform
        df = pd.read_sql(self.__script, self.__dbEngine, parse_dates=['date'])

        # Partition based on station_id
        if station_id != None:
            df = df[df['station_id'] == station_id]

        nun = df['station_id'].nunique()
        for i, sid in enumerate(df['station_id'].unique()):
            print(f'{i+1}/{nun} - Processing...', end='\r')
            save_path = os.path.join(config.OUTPUT_PATH, f'weather_cleaned_{sid}.csv')
            df.to_csv(save_path, index=False)
            print(f'{i+1}/{nun} - Cleaned Data saved to {save_path}')

if __name__ == '__main__':
    dt = WeatherDataTransformer()
    dt.run()


