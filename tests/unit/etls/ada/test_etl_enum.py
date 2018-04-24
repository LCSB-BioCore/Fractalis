"""This module provides tests for the enum ETL for Ada."""

import json

import pytest
import responses

from fractalis.data.etls.ada.etl_enum import EnumETL


# noinspection PyMissingOrEmptyDocstring,PyMissingTypeHints
class TestEnumETL:

    etl = EnumETL()

    valid_descriptor = {
        'dictionary': {
            'name': 'foo',
            'projection': 'foo',
            'label': 'bar',
            'fieldType': 'Enum',
            'numValues': {'0': 'abc', '1': 'def'},
            'isArray': False
        },
        'data_set': 'baz'
    }

    def test_correct_handler(self):
        assert self.etl.can_handle(handler='ada', descriptor={
            'dictionary': {'fieldType': 'Enum', 'isArray': False}
        })
        assert not self.etl.can_handle(handler='ada', descriptor={
            'dictionary': {'fieldType': 'Enum', 'isArray': True}
        })
        assert not self.etl.can_handle(handler='transmart', descriptor={
            'dictionary': {'fieldType': 'Enum', 'isArray': False}
        })
        assert not self.etl.can_handle(handler='ada', descriptor={
            'dictionary': {'fieldType': 'foo', 'isArray': False}
        })

    def test_extract_raises_readable_if_not_200(self):
        with responses.RequestsMock() as response:
            response.add(response.GET,
                         'http://foo.bar/dataSets/records/findCustom',
                         body='{}',
                         status=400,
                         content_type='application/json')
            with pytest.raises(ValueError) as e:
                self.etl.extract(server='http://foo.bar',
                                 token='',
                                 descriptor=self.valid_descriptor)
                assert '[400]' in e

    def test_extract_raises_readable_if_not_json(self):
        with responses.RequestsMock() as response:
            response.add(response.GET,
                         'http://foo.bar/dataSets/records/findCustom',
                         body='abc{//}',
                         status=200,
                         content_type='application/json')
            with pytest.raises(TypeError) as e:
                self.etl.extract(server='http://foo.bar',
                                 token='',
                                 descriptor=self.valid_descriptor)
                assert 'unexpected data' in e

    def test_returns_data_for_correct_input(self):
        body = json.dumps([{'foo': 0, '_id': {'$oid': '12345'}}])
        with responses.RequestsMock() as response:
            response.add(response.GET,
                         'http://foo.bar/dataSets/records/findCustom',
                         body=body,
                         status=200,
                         content_type='application/json')
            raw_data = self.etl.extract(server='http://foo.bar',
                                        token='',
                                        descriptor=self.valid_descriptor)
            assert isinstance(raw_data, list)

    def test_returns_df_for_correct_data(self):
        body = json.dumps([{'foo': 0, '_id': {'$oid': '12345'}}])
        with responses.RequestsMock() as response:
            response.add(response.GET,
                         'http://foo.bar/dataSets/records/findCustom',
                         body=body,
                         status=200,
                         content_type='application/json')
            raw_data = self.etl.extract(server='http://foo.bar',
                                        token='',
                                        descriptor=self.valid_descriptor)
            df = self.etl.transform(raw_data=raw_data,
                                    descriptor=self.valid_descriptor)
            assert df.shape == (1, 3)
            assert list(df['id']) == ['12345']
            assert list(df['feature']) == ['bar']
            assert list(df['value']) == ['abc']
