"""This module provides sample data."""

import pandas as pd
import random

from fractalis.data.etl import ETL


class RandomCategoricalETL(ETL):

    name = 'test_categorical_etl'
    produces = 'categorical'

    @staticmethod
    def can_handle(handler: str, descriptor: dict) -> bool:
        return handler == 'test' and \
               descriptor['data_type'] == 'categorical'

    def extract(self, server: str,
                token: str, descriptor: dict) -> pd.DataFrame:
        data = pd.DataFrame([random.choice(descriptor['values'])
                             for i in range(descriptor['num_samples'])])
        return data

    def transform(self, raw_data: pd.DataFrame,
                  descriptor: dict) -> pd.DataFrame:
        raw_data.insert(0, 'id', raw_data.index.astype('str'))
        df = pd.melt(raw_data, id_vars='id', var_name='feature')
        return df
