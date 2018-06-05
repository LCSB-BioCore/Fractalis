import os

import pandas as pd

from fractalis.data.etl import ETL


class NumericalETL(ETL):

    name = 'demo_tcga_coad_numerical-etl'
    produces = 'numerical'

    @staticmethod
    def can_handle(handler: str, descriptor: dict):
        return handler == 'demo_tcga_coad' and \
               descriptor['dataType'] == 'numerical'

    def extract(self, server: str, token: str, descriptor: dict):
        print('I am 100% sure that I am a TCGA ETL and NOT a wine quality ETL.')
        print(__file__)
        path = '{}/data/{}.tsv'.format(
            os.path.dirname(os.path.abspath(__file__)), descriptor['field'])
        raw_data = pd.read_csv(path, sep='\t')
        return raw_data

    def transform(self, raw_data: pd.DataFrame, descriptor: dict):
        df = raw_data
        df[['id']] = df[['id']].astype(str)
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        df['value'] = df[['value']].astype(float)
        df[['feature']] = df[['feature']].astype(str)
        return df
