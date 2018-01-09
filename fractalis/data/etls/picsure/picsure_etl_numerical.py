"""Provides NumericalETL for PIC-SURE API."""

import json
from typing import List

import pandas as pd
from io import StringIO

from fractalis.data.etl import ETL
from fractalis.data.etls.picsure import shared


class NumericalETL(ETL):
    """NumericalETL implements support for PIC-SURE 'numerical' type."""

    name = 'pic-sure_numerical_etl'
    produces = 'numerical'

    @staticmethod
    def can_handle(handler: str, descriptor: dict) -> bool:
        return handler == 'pic-sure' and \
               descriptor['dataType'] == 'numerical'

    def extract(self, server: str, token: str, descriptor: dict) -> List[str]:
        result_id = shared.submit_query(
            query=json.dumps(descriptor['query']), server=server, token=token)
        shared.wait_for_completion(
            result_id=result_id, server=server, token=token)
        raw_data = shared.get_data(
            result_id=result_id, server=server, token=token)
        return raw_data

    def transform(self, raw_data: List[dict],
                  descriptor: dict) -> pd.DataFrame:
        df = pd.read_csv(StringIO(raw_data))
        feature = df.columns[1]
        df.columns = ['id', 'value']
        df.insert(1, 'feature', feature)
        df[['id']] = df[['id']].astype(str)
        df[['value']] = df[['value']].astype(float)
        return df
