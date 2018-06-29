"""This module provides test for the numerical data ETL for tranSMART"""

import json

import pytest
import responses

from fractalis.data.etls.transmart.etl_numerical import NumericalETL


# noinspection PyMissingOrEmptyDocstring,PyMissingTypeHints
class TestNumericalETL:

    etl = NumericalETL()
    descriptor = dict(
        constraint='',
        data_type='numerical'
    )

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
                                 token='', descriptor=self.descriptor)
                assert '[400]' in e

    def test_extract_raises_readable_if_not_json(self):
        with responses.RequestsMock() as response:
            response.add(response.GET, 'http://foo.bar/v2/observations',
                         body='123{//}',
                         status=200,
                         content_type='application/json')
            with pytest.raises(ValueError) as e:
                self.etl.extract(server='http://foo.bar',
                                 token='', descriptor=self.descriptor)
                assert 'unexpected data' in e

    def test_extract_works_for_valid_input(self):
        with responses.RequestsMock() as response:
            response.add(response.GET, 'http://foo.bar/v2/observations',
                         body='{}',
                         status=200,
                         content_type='application/json')
            raw_data = self.etl.extract(server='http://foo.bar',
                                        token='', descriptor=self.descriptor)
            assert isinstance(raw_data, dict)

    def test_transform_valid_input_correct_output(self):
        body = {
            "cells": [{"inlineDimensions": ["292278994-08-16T23:00:00Z", None, "@"], "dimensionIndexes": [0, 0, 0, None, 0, None, None], "numericValue": 52.0}],  # noqa: 501
            "dimensionElements": {"patient": [{"id": 1000421548, "deathDate": None, "birthDate": None, "race": None, "maritalStatus": None, "inTrialId": "3052", "age": 52, "trial": "GSE4382", "sexCd": None, "sex": "unknown", "religion": None}]}  # noqa: 501
        }
        with responses.RequestsMock() as response:
            response.add(response.GET, 'http://foo.bar/v2/observations',
                         body=json.dumps(body),
                         status=200,
                         content_type='application/json')

            raw_data = self.etl.extract(server='http://foo.bar',
                                        token='', descriptor=self.descriptor)
            df = self.etl.transform(raw_data=raw_data, descriptor=self.descriptor)
            assert df.shape == (1, 3)
            assert df.values.tolist() == [['3052', 'value', 52.0]]
            assert list(df) == ['id', 'feature', 'value']
