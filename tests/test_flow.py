import os
import sys
sys.path.append('..')

import sqlalchemy

import modules.input_pipeline.flow as flow

def test_instantiate():
    engine = sqlalchemy.create_engine(f'sqlite:///data/weather.db')
    engine.connect()
    dt = flow.WeatherDataTransformer(engine)

def test_static_run_once():
    engine = sqlalchemy.create_engine(f'sqlite:///data/weather.db')
    dt = flow.WeatherDataTransformer(engine)
    dt.run(96653)

def test_static_run_all():
    engine = sqlalchemy.create_engine(f'sqlite:///data/weather.db')
    dt = flow.WeatherDataTransformer(engine)
    dt.run()
