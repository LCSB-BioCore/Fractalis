"""Provides NumericalETL for wine quality demo data set."""

import os

import pandas as pd

from fractalis.data.etl import ETL


class NumericalETL(ETL):
    """NumericalETL implements support for the
    numerical data of the wine quality data set. """

    name = 'demo-wine-quality-numerical-etl'
    produces = 'numerical'

    @staticmethod
    def can_handle(handler: str, descriptor: dict):
        return handler == 'demo' and descriptor['dataType'] == 'numerical'

    def extract(self, server: str, token: str, descriptor: dict):
        path = os.path.dirname(os.path.abspath(__file__)) + '/wine_quality.csv'
        csv = pd.read_csv(path, sep='\t')
        raw_data = csv[descriptor['field']]
        return raw_data

    def transform(self, raw_data: pd.DataFrame, descriptor: dict):
        df = raw_data.reset_index(level=0)
        df['feature'] = descriptor['field']
        df.columns = ['id', 'value', 'feature']
        df[['id']] = df[['id']].astype(str)
        df[['value']] = df[['value']].astype(float)
        df[['feature']] = df[['feature']].astype(str)
        return df
