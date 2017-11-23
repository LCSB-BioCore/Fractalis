"""This module provides tests for the misc controller."""

import re

import flask
import pytest


class TestMisc:

    @pytest.fixture(scope='function')
    def test_client(self):
        from fractalis import app
        app.testing = True
        with app.test_client() as test_client:
            yield test_client

    def test_get_version_returns_version(self, test_client):
        rv = test_client.get('/misc/version')
        body = flask.json.loads(rv.get_data())
        assert re.match('^\d+.\d+.\d+$', body['version'])
