"""This module provides tests for the ada etl handler."""

import pytest

from fractalis.data.etls.ada.handler_ada import AdaHandler


# noinspection PyMissingOrEmptyDocstring,PyMissingTypeHints
class TestAdaHandler:

    @pytest.fixture(scope='function', params=[
        {'server': 'http://foo.bar', 'auth': ''},
        {'server': 'http://foo.bar', 'auth': {}},
        {'server': 'http://foo.bar', 'auth': {'abc': 'abc'}},
        {'server': 'http://foo.bar', 'auth': {'token': ''}},
        {'server': 'http://foo.bar', 'auth': {'token': object}},
        {'server': 'http://foo.bar', 'auth': {'user': 'foo'}},
        {'server': 'http://foo.bar', 'auth': {'passwd': 'foo'}},
        {'server': 'http://foo.bar', 'auth': {'user': '', 'passwd': ''}},
        {'server': 'http://foo.bar', 'auth': {'user': 'foo', 'passwd': ''}},
        {'server': 'http://foo.bar', 'auth': {'user': '', 'passwd': 'foo'}},
        {'server': 'http://foo.bar',
         'auth': {'user': '', 'passwd': '', 'foo': 'bar'}},
        {'server': '', 'auth': {'token': 'foo'}},
        {'server': object, 'auth': {'token': 'foo'}}
    ])
    def bad_init_args(self, request):
        return request.param

    def test_throws_if_bad_init_args(self, bad_init_args):
        with pytest.raises(ValueError):
            AdaHandler(**bad_init_args)