import os

import pandas as pd

from fractalis.data.etl import ETL


class NumericalETL(ETL):

    name = 'demo_tca_coad_numerical-etl'
    produces = 'numerical_array'

    @staticmethod
    def can_handle(handler: str, descriptor: dict):
        return handler == 'demo_tcga_coad' and \
               descriptor['dataType'] == 'numerical_array'

    def extract(self, server: str, token: str, descriptor: dict):
        path = '{}/data/{}.tsv'.format(
            os.path.dirname(os.path.abspath(__file__)), descriptor['field'])
        raw_data = pd.read_csv(path, sep='\t')
        return raw_data

    def transform(self, raw_data: pd.DataFrame, descriptor: dict):
        df = raw_data
        df[['id']] = df[['id']].astype(str)
        df[['value']] = df[['value']].astype(float)
        df[['feature']] = df[['feature']].astype(str)
        return df
