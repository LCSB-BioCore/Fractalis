"""Default ETL to get data from ADA"""

import requests
from pandas import DataFrame

from fractalis.data.etl import ETL


class DefaultETL(ETL):
    """The default ETL for ADA."""

    name = 'ada_default_etl'
    _handler = 'ada'
    _data_type = 'default'

    def extract(self, server: str,
                token: str, descriptor: dict) -> object:
        data_set = descriptor['data_set']
        projections = ['_id']  # we always ask for an id
        projections += [descriptor['projection']]
        split = token.split('=')
        assert len(split) == 2
        cookie = {split[0]: split[1][1:-1]}
        r = requests.get(url='{}/studies/records/findCustom'.format(server),
                         headers={'Accept': 'application/json'},
                         params={
                             'dataSet': data_set,
                             'projection': projections
                         },
                         cookies=cookie)
        if r.status_code != 200:
            raise ValueError("Data extraction failed. Reason: [{}]: {}"
                             .format(r.status_code, r.text))
        return r.json()

    def transform(self, raw_data) -> DataFrame:
        data = []
        for row in raw_data:
            id = row['_id']['$oid']
            del row['_id']
            row['id'] = id
            data.append(row)
        df = DataFrame(data)
        return df
