"""Default ETL to get data from ADA"""

from pandas import DataFrame

from fractalis.data.etl import ETL


class DefaultETL(ETL):

    name = 'ada_default_etl'
    _handler = 'ada'
    _data_type = 'default'

    def extract(self, server: str, token: str, descriptor: dict) -> object:
        pass

    def transform(self, raw_data: object) -> DataFrame:
        pass