"""Provides DoubleETL for Ada."""

from typing import List

from pandas import DataFrame

from fractalis.data.etl import ETL
from fractalis.data.etls.ada import common


class DoubleETL(ETL):
    """DoubleETL implements support for Adas 'Enum' type"""

    name = 'ada_double_etl'
    produces = 'numerical'

    @staticmethod
    def can_handle(handler, descriptor):
        return handler == 'ada' and \
               descriptor['dictionary']['fieldType'] and \
               descriptor['dictionary']['fieldType'] == 'Double'

    def extract(self, server: str, token: str, descriptor: dict) -> List[dict]:
        data_set = descriptor['data_set']
        projections = ['_id']
        projections += [descriptor['dictionary']['projection']]
        cookie = common.make_cookie(token=token)
        data = common.get_field(server=server, data_set=data_set,
                                cookie=cookie, projections=projections)
        return data

    def transform(self, raw_data: List[dict], descriptor: dict) -> DataFrame:
        data = common.prepare_ids(raw_data)
        data = common.name_to_label(data, descriptor)
        data_frame = DataFrame(data)
        return data_frame

