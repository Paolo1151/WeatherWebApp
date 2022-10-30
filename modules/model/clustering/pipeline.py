import sys
import os
sys.path.append(os.path.join('..', '..'))

import pandas as pd

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

import joblib

import config as cfg
from modules.base.connector import DatabaseConnector

class ClusteringPipeline(DatabaseConnector):
    def __init__(self):
        super().__init__()
        self.__pipe = Pipeline([
            ('scaler', StandardScaler()),
            ('cluster', KMeans())
        ])

    def run(self):
        self._preprocess()
        print('Preprocessing Done!...')
        self._model()
        print('Model Training Done!...')
        self._generate()
        print('Model Generation Done!...')

    def _preprocess(self):
        self._extract_data()
        self._prepare_data()

    def _model(self):
        self._train_pipeline()

    def _generate(self):
        self._save_model()

    def _extract_data(self):
        self.__df = pd.read_sql(
            'SELECT * FROM weather_schema.climate', 
            self.get_engine()
        )

    def _prepare_data(self):
        self.__df.drop(columns=[
            # Unnecessary Columns
            'maxwindspeed',
            'avgwindspeed',
            'winddirection',
            'mostwinddirection',

            # Deleting Ids
            'entryid',
            'dateid',
            'stationid',
            'provinceid'
        ], inplace=True)
        self.__df.dropna(inplace=True)

    # Modelling
    def _train_pipeline(self):
        self.__pipe.fit(self.__df)

    # Saving Model
    def _save_model(self):
        joblib.dump(self.__pipe, os.path.join(cfg.MODELS_PATH, 'ClusteringModel.joblib'))



