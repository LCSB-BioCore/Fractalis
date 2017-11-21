"""Provides DoubleETL for Ada."""

from typing import List

from pandas import DataFrame

from fractalis.data.etl import ETL
from fractalis.data.etls.ada import shared


class DoubleETL(ETL):
    """DoubleETL implements support for Adas 'Enum' type"""

    name = 'ada_double_etl'
    produces = 'numerical'

    @staticmethod
    def can_handle(handler, descriptor):
        return handler == 'ada' and \
               descriptor['dictionary']['fieldType'] and \
               descriptor['dictionary']['fieldType'] == 'Double' and not \
               descriptor['dictionary']['isArray']

    def extract(self, server: str, token: str, descriptor: dict) -> List[dict]:
        data_set = descriptor['data_set']
        projection = descriptor['dictionary']['projection']
        cookie = shared.make_cookie(token=token)
        data = shared.get_field(server=server, data_set=data_set,
                                cookie=cookie, projection=projection)
        return data

    def transform(self, raw_data: List[dict], descriptor: dict) -> DataFrame:
        data = shared.prepare_ids(raw_data)
        data = shared.name_to_label(data, descriptor)
        df = shared.make_data_frame(data)
        return df
