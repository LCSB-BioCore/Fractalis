from fractalis.data.etls.etl import ETL


class FooETL(ETL):

    name = 'test_foo_task'
    _HANDLER = 'test'
    _DATA_TYPE = 'foo'

    def extract(self, params):
        pass

    def transform(self, raw_data):
        pass
