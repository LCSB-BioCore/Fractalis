"""This module provides test for the numerical data ETL for tranSMART"""

import pytest
import responses

from fractalis.data.etls.transmart.etl_numerical import NumericalETL


# noinspection PyMissingOrEmptyDocstring,PyMissingTypeHints
class TestNumericalETL:

    etl = NumericalETL()

    def test_correct_handler(self):
        assert self.etl.can_handle(handler='transmart',
                                   descriptor={'data_type': 'numerical'})
        assert not self.etl.can_handle(handler='ada',
                                       descriptor={'data_type': 'numerical'})
        assert not self.etl.can_handle(handler='ada',
                                       descriptor={'data_type': 'categorical'})
        assert not self.etl.can_handle(handler='ada',
                                       descriptor={'foo': 'bar'})

    def test_extract_raises_readable_if_not_200(self):
        with responses.RequestsMock() as response:
            response.add(response.GET, 'http://foo.bar/v2/observations',
                         body='{}',
                         status=400,
                         content_type='application/json')
            with pytest.raises(ValueError) as e:
                self.etl.extract(server='http://foo.bar',
                                 token='', descriptor={'path': ''})
                assert '[400]' in e

    def test_extract_raises_readable_if_not_json(self):
        with responses.RequestsMock() as response:
            response.add(response.GET, 'http://foo.bar/v2/observations',
                         body='123{//}',
                         status=200,
                         content_type='application/json')
            with pytest.raises(ValueError) as e:
                self.etl.extract(server='http://foo.bar',
                                 token='', descriptor={'path': ''})
                assert 'unexpected data' in e

    def test_extract_works_for_valid_input(self):
        with responses.RequestsMock() as response:
            response.add(response.GET, 'http://foo.bar/v2/observations',
                         body='{}',
                         status=200,
                         content_type='application/json')
            raw_data = self.etl.extract(server='http://foo.bar',
                                        token='', descriptor={'path': ''})
            assert isinstance(raw_data, dict)
