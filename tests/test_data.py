"""This module tests the data controller module."""

import os
from uuid import UUID

import flask
import pytest

from fractalis import redis
from fractalis import app


class TestData:

    @pytest.fixture(scope='function')
    def test_client(self):
        from fractalis import app
        app.testing = True
        with app.test_client() as test_client:
            yield test_client
            redis.flushall()

    @pytest.fixture(scope='function')
    def post(self, test_client):
        return test_client.post('/data', data=flask.json.dumps(dict(
            handler='test',
            server='localhost:1234',
            token='7746391376142672192764',
            descriptors=[
                {
                    'data_type': 'foo',
                    'concept': 'abc//efg/whatever'
                }
            ]
        )))

    # POST /

    def test_201_on_POST_and_file_exists(self, test_client, post):
        rv = post
        body = flask.json.loads(rv.get_data())
        assert len(body['data_ids']) == 1
        data_id = body['data_ids'][0]
        assert UUID(data_id)
        data_dir = os.path.join(app.config['FRACTALIS_TMP_DIR'], 'data')
        assert len(os.listdir(data_dir)) == 1
        assert UUID(os.listdir(data_dir)[0])
