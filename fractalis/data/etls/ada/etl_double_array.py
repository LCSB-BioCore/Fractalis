"""Provides DoubleArrayETL for Ada."""

from typing import List

import pandas as pd

from fractalis.data.etl import ETL
from fractalis.data.etls.ada import shared


class DoubleArrayETL(ETL):
    """DoubleArrayETL implements support for Adas 'Enum' array type."""

    name = 'ada_double_array_etl'
    produces = 'numerical_array'

    @staticmethod
    def can_handle(handler, descriptor):
        return handler == 'ada' and \
               descriptor['dictionary']['fieldType'] and \
               descriptor['dictionary']['fieldType'] == 'Double' and \
               descriptor['dictionary']['isArray']

    def extract(self, server: str, token: str, descriptor: dict) -> List[dict]:
        data_set = descriptor['data_set']
        projection = descriptor['dictionary']['projection']
        cookie = shared.make_cookie(token=token)
        data = shared.get_field(server=server, data_set=data_set,
                                cookie=cookie, projection=projection)
        return data

    def transform(self, raw_data: List[dict],
                  descriptor: dict) -> pd.DataFrame:
        name = descriptor['dictionary']['name']
        ids = []
        values = []
        for row in raw_data:
            ids.append(row['_id']['$oid'])
            values.append(row[name])
        df = pd.DataFrame(values)
        df.insert(0, 'id', ids)
        df = pd.melt(df, id_vars='id', var_name='feature')
        return df
