"""Provides StringETL for Ada."""

from typing import List

from pandas import DataFrame

from fractalis.data.etl import ETL
from fractalis.data.etls.ada import common


class StringETL(ETL):
    """StringETL implements support for Adas 'String' type"""

    name = 'ada_string_etl'
    produces = 'categorical'

    @staticmethod
    def can_handle(handler, descriptor):
        return handler == 'ada' and \
               descriptor['dictionary']['fieldType'] and \
               descriptor['dictionary']['fieldType'] == 'String'

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
