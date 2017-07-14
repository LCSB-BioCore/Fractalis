"""Provides numerical concept ETL for tranSMART."""

from typing import List

import requests
from pandas import DataFrame

from fractalis.data.etl import ETL


class NumericalETL(ETL):
    """NumericalETL implements support for tranSMARTs 'numerical' type."""

    name = 'transmart_numerical_etl'
    produces = 'numerical'

    @staticmethod
    def can_handle(handler: str, descriptor: dict) -> bool:
        return handler == 'transmart' and descriptor['dataType'] == 'numerical'

    def extract(self, server: str, token: str, descriptor: dict) -> List[dict]:
        r = requests.get(url='{}/v2/observations'
                             '?constraint={"type":"concept","path":{}}'
                             '&type=clinical'.format(server,
                                                     descriptor['path']),
                         headers={'Accept': 'application/json',
                                  'Authorization': 'Bearer {}'.format(token)})
        if r.status_code != 200:
            raise ValueError(
                "Data extraction failed. Target server responded with "
                "status code {}.".format(r.status_code))
        return r

    def transform(self, raw_data: List[dict], descriptor: dict) -> DataFrame:
        return raw_data