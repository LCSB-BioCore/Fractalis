from fractalis.data.etls.etl import ETL


class FooETL(ETL):

    _HANDLER = 'test'
    _DATA_TYPE = 'foo'

    def run(self):
        pass
