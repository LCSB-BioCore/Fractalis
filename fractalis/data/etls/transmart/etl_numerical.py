"""Provides numerical concept ETL for tranSMART."""

import requests
from pandas import DataFrame

from fractalis.data.etl import ETL


class NumericalETL(ETL):
    """NumericalETL implements support for tranSMARTs 'numerical' type."""

    name = 'transmart_numerical_etl'
    produces = 'numerical'

    @staticmethod
    def can_handle(handler: str, descriptor: dict) -> bool:
        return handler == 'transmart' and descriptor['data_type'] == 'numerical'

    def extract(self, server: str, token: str, descriptor: dict) -> dict:
        r = requests.get(url='{}/v2/observations'.format(server),
                         params={
                             'constraint': '{{"type": "concept", "path": "{}"}}'.format(descriptor["path"]),
                             'type': 'clinical'
                         },
                         headers={
                             'Accept': 'application/json',
                             'Authorization': 'Bearer {}'.format(token)
                         })
        if r.status_code != 200:
            raise ValueError(
                "Data extraction failed. Target server responded with "
                "status code {}.".format(r.status_code))
        return r.json()

    def transform(self, raw_data: dict, descriptor: dict) -> DataFrame:
        rows = []
        for entry in raw_data['cells']:
            idx = entry['dimensionIndexes'][2]
            id = raw_data['dimensionElements']['patient'][idx]['inTrialId']
            value = entry['numericValue']
            rows.append([id, value])
        df = DataFrame(rows, columns=['id', 'value'])
        return df
