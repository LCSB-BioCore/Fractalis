"""This module provides sample data."""

import pandas as pd
import numpy as np

from fractalis.data.etl import ETL


class RandomNumericalETL(ETL):

    name = 'test_numerical_array_etl'
    produces = 'numerical_array'

    @staticmethod
    def can_handle(handler: str, descriptor: dict) -> bool:
        return handler == 'test' and \
               descriptor['data_type'] == 'numerical_array'

    def extract(self, server: str,
                token: str, descriptor: dict) -> pd.DataFrame:
        data = pd.DataFrame(np.random.randn(50000, 200).tolist())
        return data

    def transform(self, raw_data: pd.DataFrame,
                  descriptor: dict) -> pd.DataFrame:
        raw_data.insert(0, 'id', raw_data.index.astype('str'))
        df = pd.melt(raw_data, id_vars='id', var_name='feature')
        return df
