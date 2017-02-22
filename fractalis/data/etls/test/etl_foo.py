from fractalis.data.etls.etl import ETL


class FooETL(ETL):

    name = 'test_foo_task'
    _HANDLER = 'test'
    _DATA_TYPE = 'foo'

    def run(self):
        return 42
