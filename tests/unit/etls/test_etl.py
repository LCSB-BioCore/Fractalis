"""This module provides test for the 'etl' module."""

import json

import pandas as pd

from fractalis import redis
from fractalis.data.etl import ETL


# noinspection PyMissingOrEmptyDocstring
class Request:

    id = 123


# noinspection PyMissingOrEmptyDocstring
class RequestStackDummy:

    top = Request()


# noinspection PyMissingOrEmptyDocstring
class MockETL(ETL):
    @property
    def name(self) -> str:
        return ''

    def transform(self, raw_data: object, descriptor: dict) -> pd.DataFrame:
        pass

    @staticmethod
    def can_handle(handler: str, descriptor: dict) -> bool:
        pass

    def extract(self, server: str, token: str, descriptor: dict) -> object:
        pass

    @property
    def produces(self) -> str:
        return ''


# noinspection PyMissingOrEmptyDocstring, PyMissingTypeHints
class TestETL:

    etl = MockETL()
    etl.request_stack = RequestStackDummy()

    def test_update_redis(self):
        df1 = pd.DataFrame([[1, 2, 3]], columns=['id', 'feature', 'value'])
        df2 = pd.DataFrame([[1, 3]], columns=['id', 'value'])
        df3 = pd.DataFrame([], columns=['id', 'feature', 'value'])
        redis.set('data:123', json.dumps({'meta': {}}))

        self.etl.update_redis(data_frame=df1)
        data_state = json.loads(redis.get('data:123'))
        assert data_state['meta']['features'] == [2]

        self.etl.update_redis(data_frame=df2)
        data_state = json.loads(redis.get('data:123'))
        assert data_state['meta']['features'] == []

        self.etl.update_redis(data_frame=df3)
        data_state = json.loads(redis.get('data:123'))
        assert data_state['meta']['features'] == []
