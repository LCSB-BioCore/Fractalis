"""Provides BooleanETL for Ada."""

from pandas import DataFrame

from fractalis.data.etl import ETL
from fractalis.data.etls.ada import common


class BooleanETL(ETL):
    """DateETL implements support for Adas 'Boolean' type"""

    name = 'ada_boolean_etl'
    _handler = 'ada'
    _accepts = 'Boolean'
    produces = 'categorical'

    def extract(self, server: str,
                token: str, descriptor: dict) -> dict:
        data_set = descriptor['data_set']
        projections = ['_id']
        projections += [descriptor['projection']]
        cookie = common.make_cookie(token=token)
        data = common.get_field(server=server, data_set=data_set,
                                cookie=cookie, projections=projections)
        dictionary = common.get_dictionary(server=server, data_set=data_set,
                                           descriptor=descriptor, cookie=cookie)
        return {'data': data, 'dictionary': dictionary}

    def transform(self, raw_data) -> DataFrame:
        data = raw_data['data']
        dictionary = raw_data['dictionary']
        data = common.prepare_ids(data)
        data = common.name_to_label(data, dictionary)
        data_frame = DataFrame(data)
        return data_frame
