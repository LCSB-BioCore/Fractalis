"""This module provides tests for the ada etl handler."""

import pytest
import responses

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

    @staticmethod
    def request_callback(request):
        headers = {
            'Set-Cookie': 'PLAY2AUTH_SESS_ID=foo-token'}
        body = ''
        return 200, headers, body

    def test_returns_token_for_credentials(self):
        with responses.RequestsMock() as response:
            response.add_callback(response.POST, 'http://foo.bar/login',
                                  callback=self.request_callback,
                                  content_type='application/json')
            adah = AdaHandler(server='http://foo.bar',
                              auth={'user': 'foo', 'passwd': 'bar'})
            assert adah._token == 'foo-token'

    def test_auth_raises_exception_for_non_json_return(self):
        with responses.RequestsMock() as response:
            response.add(response.POST, 'http://foo.bar/login',
                         body='123{//}',
                         status=200,
                         content_type='application/json')
            with pytest.raises(ValueError) as e:
                AdaHandler(server='http://foo.bar',
                           auth={'user': 'foo', 'passwd': 'bar'})
                assert 'unexpected response' in e

    def test_auth_raises_exception_for_non_200_return(self):
        with responses.RequestsMock() as response:
            response.add(response.POST, 'http://foo.bar/login',
                         body='something',
                         status=400,
                         content_type='application/json')
            with pytest.raises(ValueError) as e:
                AdaHandler(server='http://foo.bar',
                           auth={'user': 'foo', 'passwd': 'bar'})
                assert '[400]' in e
