import pandas as pd
import numpy as np

from fractalis.data.etls.etl import ETL


class FooETL(ETL):

    name = 'test_foo_task'
    _HANDLER = 'test'
    _DATA_TYPE = 'foo'

    def extract(self, server, token, descriptor):
        fake_raw_data = np.random.randn(10, 5)
        return fake_raw_data

    def transform(self, raw_data):
        fake_df = pd.DataFrame(raw_data)
        return fake_df
