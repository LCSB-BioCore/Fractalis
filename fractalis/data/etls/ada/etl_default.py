"""Default ETL to get data from ADA"""

import requests
from pandas import DataFrame

from fractalis.data.etl import ETL


class DefaultETL(ETL):

    name = 'ada_default_etl'
    _handler = 'ada'
    _data_type = 'default'

    def extract(self, server: str, token: str, descriptor: dict) -> object:
        data_set = descriptor['data_set']
        projection = descriptor['projection']
        split = token.split('=')
        assert len(split) == 2
        cookie = {split[0]: split[1][1:-1]}
        r = requests.get(url='{}/studies/records/findCustom'.format(server),
                         headers={'Accept': 'application/json'},
                         params={'dataSet': data_set, 'projection': projection},
                         cookies=cookie)
        assert r.status_code == 200
        return r.json()

    def transform(self, raw_data: object) -> DataFrame:
        df = DataFrame(raw_data)
        return df
