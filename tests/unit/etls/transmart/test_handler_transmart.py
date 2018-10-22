"""This module provides tests for the transmart etl handler."""

import pytest
import responses

from fractalis.data.etls.transmart.handler_transmart import TransmartHandler


# noinspection PyMissingOrEmptyDocstring,PyMissingTypeHints
class TestTransmartHandler:

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
        {'server': object, 'auth': {'token': 'foo'}},
        {'server': 'http://foo.bar', 'auth': {'user': 'foo', 'passwd': 'bar'}},
        {'server': 'http://foo.bar', 'auth': {'user': 'foo', 'passwd': 'bar', 'authServiceType': ''}},
        {'server': 'http://foo.bar', 'auth': {'user': 'foo', 'passwd': 'bar', 'authServiceType': 'random_name'}},
        {'server': 'http://foo.bar', 'auth': {'user': 'foo', 'passwd': 'bar', 'authServiceType': 'Oidc'}},
        {'server': 'http://foo.bar', 'auth': {'user': 'foo', 'passwd': 'bar', 'authServiceType': 'oidc',
                                              'oidcClientId': '', 'oidcServerUrl': 'http://foo.bar/oauth/token'}},
        {'server': 'http://foo.bar', 'auth': {'user': 'foo', 'passwd': 'bar', 'authServiceType': 'oidc',
                                              'oidcServerUrl': 'http://foo.bar/oauth/token'}},
        {'server': 'http://foo.bar', 'auth': {'user': 'foo', 'passwd': 'bar', 'authServiceType': 'oidc',
                                              'oidcClientId': 'test_client', 'oidcServerUrl': ''}},
        {'server': 'http://foo.bar', 'auth': {'user': 'foo', 'passwd': 'bar', 'authServiceType': 'oidc',
                                              'oidcClientId': 'test_client'}}
    ])
    def bad_init_args(self, request):
        return request.param

    def test_throws_if_bad_init_args(self, bad_init_args):
        with pytest.raises(ValueError):
            TransmartHandler(**bad_init_args)

    def test_auth_raises_exception_for_invalid_args(self):
        with pytest.raises(ValueError) as e:
            TransmartHandler(server='http://foo.bar',
                             auth={'user': 'foo', 'passwd': 'bar', 'authServiceType': 'test_server_name'})
            assert 'The authentication service type in authentication object has to be one of ' \
                   '"oidc", "transmart"' in e

    def test_returns_token_for_credentials(self):
        with responses.RequestsMock() as response:
            response.add(response.POST, 'http://foo.bar/oauth/token',
                         body='{"access_token":"foo-token","token_type":"bearer","expires_in":43185,"scope":"read write"}',  # noqa: 501
                         status=200,
                         content_type='application/json')
            tmh = TransmartHandler(server='http://foo.bar',
                                   auth={'user': 'foo', 'passwd': 'bar', 'authServiceType': 'transmart'})
            assert tmh._token == 'foo-token'

    def test_returns_token_for_credentials_oidc(self):
        with responses.RequestsMock() as response:
            response.add(response.POST, 'http://foo.bar.oidc/oauth/token',
                         body='{"access_token":"foo-token","token_type":"bearer","expires_in":43185,"scope":"read write"}',  # noqa: 501
                         status=200,
                         content_type='application/json')
            tmh = TransmartHandler(server='http://foo.bar',
                                   auth={'user': 'foo', 'passwd': 'bar', 'authServiceType': 'oidc',
                                         'oidcClientId': 'test_client',
                                         'oidcServerUrl': 'http://foo.bar.oidc/oauth/token'})
            assert tmh._token == 'foo-token'

    def test_auth_raises_exception_for_non_json_return(self):
        with responses.RequestsMock() as response:
            response.add(response.POST, 'http://foo.bar/oauth/token',
                         body='123{//}',
                         status=200,
                         content_type='application/json')
            with pytest.raises(ValueError) as e:
                TransmartHandler(server='http://foo.bar',
                                 auth={'user': 'foo', 'passwd': 'bar', 'authServiceType': 'transmart'})
                assert 'unexpected response' in e

    def test_auth_raises_exception_for_non_200_return(self):
        with responses.RequestsMock() as response:
            response.add(response.POST, 'http://foo.bar/oauth/token',
                         body='something',
                         status=400,
                         content_type='application/json')
            with pytest.raises(ValueError) as e:
                TransmartHandler(server='http://foo.bar',
                                 auth={'user': 'foo', 'passwd': 'bar', 'authServiceType': 'transmart'})
                assert '[400]' in e
