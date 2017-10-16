"""Provides highdim concept ETL for tranSMART."""

import logging

import requests
from pandas import DataFrame

from fractalis.data.etl import ETL

logger = logging.getLogger(__name__)


class HighdimETL(ETL):
    """HighdimETL implements support for tranSMARTs 'highdim' type."""

    name = 'transmart_highdim_etl'
    produces = 'highdim'

    @staticmethod
    def can_handle(handler: str, descriptor: dict) -> bool:
        return handler == 'transmart' and descriptor['data_type'] == 'highdim'

    def extract(self, server: str, token: str, descriptor: dict) -> dict:
        r = requests.get(url='{}/v2/observations'.format(server),
                         params={
                             'constraint': '{{"type": "concept","path": "{}"}}'
                                           ''.format(descriptor["path"]),
                             'projection': 'log_intensity',
                             'type': 'autodetect'
                         },
                         headers={
                             'Accept': 'application/x-protobuf',
                             'Authorization': 'Bearer {}'.format(token)
                         },
                         timeout=2000)

        if r.status_code != 200:
            error = "Target server responded with " \
                    "status code {}.".format(r.status_code)
            logger.error(error)
            raise ValueError(error)
        try:
            pass  # TODO
        except Exception as e:
            logger.exception(e)
            raise ValueError("Got unexpected data format.")

    def transform(self, raw_data: dict, descriptor: dict) -> DataFrame:
        rows = []
        for entry in raw_data['cells']:
            idx = entry['dimensionIndexes'][2]
            id = raw_data['dimensionElements']['patient'][idx]['inTrialId']
            value = entry['numericValue']
            rows.append([id, value])
        df = DataFrame(rows, columns=['id', 'value'])
        return df
